import asyncio
import re
import aiohttp
import hashlib
from charset_normalizer import from_bytes
import json
import uuid
import aiosqlite
import os
import random
from urllib.parse import urlparse
from typing import List, Dict, Optional
import trafilatura
from langdetect import detect, DetectorFactory
from langdetect.lang_detect_exception import LangDetectException

# Garante resultados consistentes na detecção de idioma
DetectorFactory.seed = 0

DB_PATH = "data/dataset.db"

BLOCKED_URL_PATTERNS = [
    "/ao-vivo", "/index", "/categoria", "/tag/",
    "/autor/", "/busca", "/search", "/page/",
]

class WaybackScraperPipeline:
    def __init__(self, target_domains: List[str], max_concurrent_requests: int = 5):
        self.target_domains = target_domains
        # Semáforo para não sobrecarregar (e ser banido) o Internet Archive
        self.semaphore = asyncio.Semaphore(max_concurrent_requests)
        self.seen_hashes = set() # Para deduplicação em memória

    async def init_db(self):
        """Abre o banco, cria a tabela (se não existir) e carrega hashes."""

        os.makedirs("data", exist_ok=True)
        self.db = await aiosqlite.connect(DB_PATH)

        with open("schema.sql", "r", encoding="utf-8") as f:
            schema = f.read()

        await self.db.executescript(schema)
        await self.db.commit()

        # Carrega todos os hashes já salvos para evitar reprocessamento
        async with self.db.execute("SELECT content_hash FROM texts") as cursor:
            rows = await cursor.fetchall()
        self.seen_hashes = {row[0] for row in rows}
        print(f"[DB] {len(self.seen_hashes)} textos já existentes carregados.")

    async def close_db(self):
        if self.db:
            await self.db.close()
    
    async def save_record(self, record: dict):
        await self.db.execute(
            """INSERT OR IGNORE INTO texts
               (text_id, content, label, broad_area, specific_theme,
                char_count, word_count, size_category, creation_date,
                source_url, source_name, content_hash)
               VALUES (?,?,?,?,?,?,?,?,?,?,?,?)""",
            (
                record["text_id"], record["content"], record["label"],
                record["broad_area"], record["specific_theme"],
                record["char_count"], record["word_count"],
                record["size_category"], record["creation_date"],
                record["source_url"], record["source_name"],
                record["content_hash"],
            )
        )
        await self.db.commit()

    async def get_cdx_snapshots(self, session: aiohttp.ClientSession, domain: str) -> List[Dict]:
        cdx_url = "http://web.archive.org/cdx/search/cdx"
        todos_snapshots = []

        for ano in range(2006, 2018):  # 2006 até 2017 inclusive
            params = {
                "url": f"{domain}/*",
                "output": "json",
                "from": str(ano),
                "to": str(ano),
                "fl": "timestamp,original,statuscode,mimetype",
                "filter": ["statuscode:200", "mimetype:text/html"],
                "collapse": "urlkey",
                "limit": "200"  # 200 por ano × 12 anos = até 2400 por domínio
            }

            for attempt in range(3):
                try:
                    timeout = aiohttp.ClientTimeout(total=45)
                    async with session.get(cdx_url, params=params, timeout=timeout) as response:
                        if response.status == 200:
                            text = await response.text()
                            if not text.strip():
                                break

                            try:
                                data = json.loads(text)
                            except json.JSONDecodeError:
                                print(f"[Erro CDX] JSON inválido para {domain} ({ano})")
                                break

                            if data and isinstance(data, list) and len(data) > 1:
                                keys = data[0]
                                snapshots_ano = [dict(zip(keys, row)) for row in data[1:]]
                                todos_snapshots.extend(snapshots_ano)
                                print(f"  {domain} ({ano}): {len(snapshots_ano)} snapshots")
                            break  # sucesso, sai do retry

                except Exception:
                    print(f"[Aviso CDX] Tentativa {attempt+1} falhou para {domain} ({ano})")
                    await asyncio.sleep((2 ** attempt) + random.uniform(0.1, 0.5))

            await asyncio.sleep(0.5)  # pausa entre anos para não sobrecarregar o CDX

        return todos_snapshots

    async def fetch_and_process_html(self, session: aiohttp.ClientSession, snapshot: Dict):
        """Baixa o HTML bruto com Retry e Backoff Exponencial, e processa o texto."""
        
        timestamp = snapshot['timestamp']
        original_url = snapshot['original']

        if any(p in original_url for p in BLOCKED_URL_PATTERNS):
            return
        
        wm_url = f"http://web.archive.org/web/{timestamp}id_/{original_url}"
        headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"}
        
        # Configurações do Retry
        max_retries = 3
        base_delay = 2 # Segundos de espera inicial

        async with self.semaphore:
            for attempt in range(max_retries):
                try:
                    timeout = aiohttp.ClientTimeout(total=30)
                    async with session.get(wm_url, timeout=timeout, headers=headers) as response:
                        
                        if response.status == 200:
                            html_bytes = await response.read()
                            
                            html_str = None
                            try:
                                detected = from_bytes(html_bytes).best()
                                if detected:
                                    html_str = str(detected)
                            except Exception as decode_err:
                                pass # Ignora falha do charset_normalizer
                                
                            if html_str:
                                await self.process_text(html_str, original_url, timestamp)
                            else:
                                await self.process_text(html_bytes.decode("utf-8", errors="replace"), original_url, timestamp)
                            
                            return # Sucesso! Sai da função e não tenta de novo.

                        # Se for um erro do servidor (5xx) ou Too Many Requests (429), vale a pena tentar de novo
                        elif response.status in [429, 500, 502, 503, 504]:
                            print(f"[Aviso] Erro {response.status} em {wm_url}. Tentativa {attempt + 1}/{max_retries}")
                        
                        # Se for 404 (Not Found) ou 403 (Forbidden), não adianta tentar de novo, a página se foi.
                        else:
                            return

                except (aiohttp.ClientError, asyncio.TimeoutError) as e:
                    print(f"[Aviso] Timeout/Conexão em {wm_url}. Tentativa {attempt + 1}/{max_retries}")

                # Se chegou aqui, a requisição falhou. Aplica o Backoff Exponencial antes de rodar o loop de novo.
                if attempt < max_retries - 1:
                    # Fórmula: base_delay * (2^attempt) + jitter aleatório
                    # Tentativa 1: espera ~2.5s | Tentativa 2: espera ~4.5s | Tentativa 3: espera ~8.5s
                    sleep_time = (base_delay * (2 ** attempt)) + random.uniform(0.1, 1.0)
                    await asyncio.sleep(sleep_time)
            
            # Se o loop terminar sem dar o "return" lá no status 200, é porque esgotou as tentativas
            print(f"[Erro] Falha definitiva após {max_retries} tentativas na URL: {wm_url}")

    def categorizar_tamanho(self, char_count: int) -> str:
        """Aplica a regra da Tabela 2 do documento do projeto."""
        if 100 <= char_count <= 600:
            return "Curto"
        elif 601 <= char_count <= 2500:
            return "Médio"
        elif 2501 <= char_count <= 5000:
            return "Médio-Longo"
        elif 5000 < char_count <= 30000:
            return "Longo"
        elif char_count > 30000:
            return "Muito Longo"
        return "Descartar" # Fora dos limites da pesquisa
    
    def  limpar_residuos(self, texto: str) -> str:
        if not isinstance(texto, str):
            return texto

        # 1. Remover URLs
        texto = re.sub(r'https?://\S+|www\.\S+', '', texto)

        # 2. Remover linhas puramente de navegação/UI
        nav_patterns = [
            r'(?i)^publicidade\.?$',
            r'(?i)^compartilhar\.?$',
            r'(?i)^clique aqui\.?$',
            r'(?i)^leia (também|mais)\.?$',
            r'(?i)^veja (também|mais)\.?$',
            r'(?i)^saiba mais\.?$',
        ]

        linhas = texto.split('\n')
        linhas_limpas = [
            l for l in linhas
            if not any(re.match(p, l.strip()) for p in nav_patterns)
        ]
        texto = '\n'.join(linhas_limpas)

        # 3. Remover padrões inline mais perigosos (sem destruir contexto)
        inline_patterns = [
            r'\bleia (mais|também):?\b',
            r'\bveja (mais|também):?\b',
            r'\bclique (aqui|em)\b',
        ]

        texto = re.sub('|'.join(inline_patterns), '', texto, flags=re.IGNORECASE)

        # 4. Normalização leve (sem destruir estrutura)
        texto = re.sub(r'[ \t]+', ' ', texto)
        texto = re.sub(r'\n{3,}', '\n\n', texto)

        return texto.strip()
    
    async def process_text(self, html_str: str, original_url: str, timestamp: str):
        """Extrai, limpa, filtra e deduplica o conteúdo."""
        # 1. Extração Polimórfica: Ignora menus, rodapés e pega o texto principal
        texto_limpo = trafilatura.extract(
            html_str, 
            url=original_url,
            target_language="pt",
            include_comments=False, 
            include_tables=False,
            favor_precision=True,
            include_images=False,
            include_links=False,
        )
        
        
        if not texto_limpo:
            return
        

        texto_limpo = self.limpar_residuos(texto_limpo)

        word_count = len(texto_limpo.split())

        linhas_totais = texto_limpo.split('\n')
        linhas_com_conteudo = [l for l in linhas_totais if l.strip()]

        if len(linhas_totais) > 0:
            ratio_vazias = 1 - len(linhas_com_conteudo) / len(linhas_totais)
            if ratio_vazias > 0.4:  # mais de 40% de linhas vazias = cheiro de menu
                return

        if not linhas_com_conteudo: return None
        palavras_por_linha = word_count / len(linhas_com_conteudo)
        if palavras_por_linha < 5:
            return

        char_count = len(texto_limpo)
        categoria = self.categorizar_tamanho(char_count)
        
        if categoria == "Descartar":
            return

        ano_criacao = timestamp[:4]

        # 3. Filtro de Idioma
        try:
            if char_count > 200:
                idioma = detect(texto_limpo)
                if idioma not in ['pt']: # Modifique conforme sua necessidade
                    return
        except LangDetectException:
            return

        # 4. Deduplicação por Hash (Evita salvar a mesma notícia/texto duas vezes)
        texto_hash = hashlib.md5(texto_limpo.encode('utf-8')).hexdigest()
        if texto_hash in self.seen_hashes:
            return
        
        self.seen_hashes.add(texto_hash)

        nome_fonte = urlparse(original_url).netloc

        # 5. Salvar na estrutura final
        record = {
            "text_id": str(uuid.uuid4()),      # Identificador único
            "content": texto_limpo,            # Conteúdo bruto limpo
            "label": 0,                        # 0 para humano
            "broad_area": None,                # A ser preenchido pelo Maritaca depois
            "specific_theme": None,            # A ser preenchido pelo Maritaca depois
            "char_count": char_count,
            "word_count": word_count,
            "size_category": categoria,
            "creation_date": ano_criacao,
            "source_url": original_url,
            "source_name": nome_fonte,
            "content_hash": texto_hash         
        }
        await self.save_record(record)
        print(f"[Sucesso] Extraído: {original_url} ({len(texto_limpo)} chars)")

    async def run(self, max_test_urls: int = None):
        await self.init_db()

        try:
            """Orquestrador do Pipeline."""
            async with aiohttp.ClientSession() as session:
                print("Iniciando Fase 1: Discovery (CDX API)...")
                all_snapshots = []
                for domain in self.target_domains:
                    snapshots = await self.get_cdx_snapshots(session, domain)
                    all_snapshots.extend(snapshots)
                    print(f"Encontrados {len(snapshots)} snapshots válidos para {domain}.")

                # Aplica a trava de teste se o parâmetro foi passado
                if max_test_urls:
                    random.shuffle(all_snapshots)
                    all_snapshots = all_snapshots[:max_test_urls]
                    print(f"Modo Teste ativado: Limitando a {max_test_urls} URLs.")

                print(f"\nFase 2/3: Processando {len(all_snapshots)} links em lotes...")
                batch_size = 100
                for i in range(0, len(all_snapshots), batch_size):
                    batch = all_snapshots[i:i+batch_size]
                    tasks = [self.fetch_and_process_html(session, s) for s in batch]
                    await asyncio.gather(*tasks)
                    # Dá um pequeno respiro para não estressar a máquina e o servidor
                    await asyncio.sleep(1)
        finally:
            await self.close_db()

        print(f"\nPipeline Finalizado! Total de textos únicos e limpos coletados")

# --- Execução do Script ---
if __name__ == "__main__":
    dominios_teste = ["g1.globo.com/ciencia-e-saude", 
                      "bbc.com/portuguese",
                      "noticias.uol.com.br",
                        "estadao.com.br",
                        "super.abril.com.br",
                        "revistagalileu.globo.com",
                        "exame.com",
                        "folha.uol.com.br",
                        "cartacapital.com.br",
                        "oglobo.globo.com",
                        "correiobraziliense.com.br",
                        "nexojornal.com.br",
                        "brasil.elpais.com",
                      ]
    
    pipeline = WaybackScraperPipeline(target_domains=dominios_teste, max_concurrent_requests=5)
    
    # 1. Executa a pipeline
    asyncio.run(pipeline.run(max_test_urls=100))
     
    print("\nOs resultados foram salvos no arquivo 'dataset_teste.json' na mesma pasta deste script.")
    print("Abra o arquivo no VS Code para verificar se a estrutura dos labels está correta!")