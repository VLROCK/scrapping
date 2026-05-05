import asyncio
import re
import aiohttp
import hashlib
from bs4 import BeautifulSoup
from charset_normalizer import from_bytes
import uuid
import aiosqlite
import os
import gzip
import random
import time
from urllib.parse import urlparse
from typing import List, Optional
import trafilatura
from langdetect import detect, DetectorFactory
from langdetect.lang_detect_exception import LangDetectException

DetectorFactory.seed = 0

# [ALTERADO] Nome do banco de dados atualizado para refletir o novo site
DB_PATH = "data/dataset_literario_recanto.db"

# ── Filtros e Configurações ──────────────────────────────────────────────────
BLOCKED_URL_PATTERNS = [
    "/ao-vivo", "/index", "/categoria", "/tag/", "/autor/", 
    "/busca", "/search", "/page/", "/galeria", "/login", "/embed",     
    "/videos_e_fotos", "/video/", "/blog/", "/platb/", "/live/", "/fotos/", "/album/",
    "/resenha", "/critica", "/ensaio", "/artigo", "/opiniao", 
    "/entrevista", "/coluna", "/noticia", "/reportagem",
    # [NOVO] Bloqueios específicos do Recanto das Letras (áudio, fóruns, etc)
    "/audios/", "/e-books/", "/mensagens/", "/homenagens/" 
]

# [ALTERADO] O Recanto não usa .html. Os textos ficam direto na pasta da categoria com um ID numérico.
URL_CERTO = [
    "/poesias/", "/contos/", "/cronicas/", "/artigos/", "/ensaios/"
]

BLACKLIST_TERMS = [
    "cookies", "termos de uso", "política de privacidade",
    "assine", "newsletter", "cadastre-se", "media player"
]

REVIEW_TERMS = [
    "publicado pela editora", "tradução de", "isbn", "páginas", 
    "nesta resenha", "neste ensaio", "neste artigo", "o autor constrói", 
    "a obra de", "o romance de", "o livro de", "projeto gráfico", 
    "literatura contemporânea", "o protagonista", "o enredo"
]

METADATA_PATTERNS = [
    r'^\s*-\s*\w+:', r'timestamp:', r'totalImagens:',
    r'fotoInicial:', r'imagePath:', r'ordem:',
]

MAX_RUNTIME_SECONDS = 5 * 60 * 60

class LiveScraperPipeline:
    def __init__(self, target_domains: List[str], max_concurrent_requests: int = 10):
        self.target_domains = target_domains
        self.semaphore = asyncio.Semaphore(max_concurrent_requests)
        self.seen_hashes: set = set()
        self.start_time: float = 0.0

    def tempo_esgotado(self) -> bool:
        return (time.monotonic() - self.start_time) > MAX_RUNTIME_SECONDS

    # ── Banco de dados ───────────────────────────────────────────────────────
    async def init_db(self):
        os.makedirs("data", exist_ok=True)
        self.db = await aiosqlite.connect(DB_PATH)
        with open("schema.sql", "r", encoding="utf-8") as f:
            await self.db.executescript(f.read())
        await self.db.commit()

        async with self.db.execute("SELECT content_hash FROM texts") as cursor:
            self.seen_hashes = {row[0] for row in await cursor.fetchall()}
        print(f"[DB] {len(self.seen_hashes)} textos já existentes carregados.")

    async def close_db(self):
        if self.db: await self.db.close()

    async def save_record(self, record: dict):
        await self.db.execute(
            """INSERT OR IGNORE INTO texts
               (text_id, content, label, broad_area, specific_theme,
                char_count, word_count, size_category, creation_date,
                source_url, source_name, content_hash)
               VALUES (?,?,?,?,?,?,?,?,?,?,?,?)""",
            (record["text_id"], record["content"], record["label"],
             record["broad_area"], record["specific_theme"],
             record["char_count"], record["word_count"],
             record["size_category"], record["creation_date"],
             record["source_url"], record["source_name"],
             record["content_hash"])
        )
        await self.db.commit()

    async def url_ja_visitada(self, url: str) -> bool:
        url_hash = hashlib.md5(url.encode()).hexdigest()
        async with self.db.execute("SELECT 1 FROM visited_urls WHERE url_hash = ?", (url_hash,)) as cur:
            return await cur.fetchone() is not None

    async def marcar_url_visitada(self, url: str):
        url_hash = hashlib.md5(url.encode()).hexdigest()
        await self.db.execute(
            "INSERT OR IGNORE INTO visited_urls (url_hash, visited_at) VALUES (?, datetime('now'))",
            (url_hash,)
        )
        await self.db.commit()

    # ── [NOVO] Verificação de Content-Signals para Inteligência Artificial ───
    async def verify_ai_content_signals(self, session: aiohttp.ClientSession, domain: str) -> bool:
        """
        Acessa o robots.txt do site e verifica se existem regras de Content-Signal
        bloqueando explicitamente ai-train ou ai-input.
        Retorna True se for permitido (ou neutro), False se for proibido.
        """
        print(f"[Ética] Verificando permissões de IA (Content-Signals) em {domain}...")
        robots_url = f"https://{domain}/robots.txt"
        
        try:
            async with session.get(robots_url, timeout=10) as resp:
                if resp.status == 200:
                    texto_robots = await resp.text()
                    
                    # Procura por linhas no formato "ai-train: no" ou "ai-input: no"
                    if re.search(r'(?i)ai-train\s*:\s*no', texto_robots):
                        print(f"[BLOQUEIO] O site {domain} proíbe treinamento de IA (ai-train: no). Abortando.")
                        return False
                    
                    if re.search(r'(?i)ai-input\s*:\s*no', texto_robots):
                        print(f"[BLOQUEIO] O site {domain} proíbe coleta para IA (ai-input: no). Abortando.")
                        return False
                        
                    print(f"[Ética] Nenhum bloqueio de IA detectado no robots.txt de {domain}.")
                    return True
                else:
                    print(f"[Ética] robots.txt não encontrado ({resp.status}). Assumindo permissão neutra.")
                    return True
        except Exception as e:
            print(f"[Ética] Falha ao ler robots.txt: {e}. Assumindo permissão neutra.")
            return True

    # ── Fase 1: Descoberta Recursiva via Sitemap ─────────────────────────────
    # ── Fase 1: Descoberta Recursiva via Sitemap ─────────────────────────────
    async def get_all_sitemap_links(self, session: aiohttp.ClientSession, domain: str) -> List[str]:
        print(f"[Discovery] Procurando mapas do site para {domain}...")
        links_artigos = set()
        
        # [ALTERADO] Adicionado o sitemap com traço (-), exatamente como o do Recanto
        sitemaps_para_visitar = [
            f"https://{domain}/sitemap.xml",
            f"https://{domain}/sitemap-index.xml", 
            f"https://{domain}/sitemap_index.xml"
        ]
        sitemaps_visitados = set()

        headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"}

        while sitemaps_para_visitar:
            url_sitemap = sitemaps_para_visitar.pop(0)
            if url_sitemap in sitemaps_visitados: continue
            sitemaps_visitados.add(url_sitemap)

            try:
                async with session.get(url_sitemap, timeout=30, headers=headers) as resp:
                    if resp.status == 200:
                        
                        # [NOVO] A Mágica do GZIP: Descompacta o arquivo se terminar em .gz
                        if url_sitemap.endswith('.gz'):
                            compressed_data = await resp.read() # Lê em binário
                            xml_data = gzip.decompress(compressed_data).decode('utf-8', errors='ignore')
                        else:
                            xml_data = await resp.text() # Lê como texto normal
                        
                        encontrados = re.findall(r'<loc>(.*?)</loc>', xml_data)
                        
                        for link in encontrados:
                            link = link.replace('<![CDATA[', '').replace(']]>', '').strip()
                            
                            # [ALTERADO] Agora ele aceita tanto .xml normal quanto .xml.gz para explorar
                            if link.endswith('.xml') or link.endswith('.xml.gz'):
                                sitemaps_para_visitar.append(link)
                            # Se for um link de texto normal do Recanto (ex: /poesias/123)
                            elif domain in link and not any(lixo in link for lixo in BLOCKED_URL_PATTERNS):
                                if any(certo in link for certo in URL_CERTO):
                                    links_artigos.add(link)
                                    
            except Exception as e:
                print(f"  [Erro Sitemap] Falha ao ler {url_sitemap}: {e}")
                pass 

        print(f"  -> {domain}: {len(links_artigos)} links totais descobertos no sitemap!")
        return list(links_artigos)
    

    # ── Fase 2 e 3: Extração de Texto ao Vivo ────────────────────────────────
    async def fetch_and_process_live(self, session: aiohttp.ClientSession, url: str):
        if self.tempo_esgotado():
            return

        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
            "Accept-Language": "pt-BR,pt;q=0.9,en-US;q=0.8,en;q=0.7",
            "Upgrade-Insecure-Requests": "1"
        }

        # print(f" -> Batendo na porta: {url}") # Omitido para não sujar muito o log.

        async with self.semaphore:
            for attempt in range(3):
                try:
                    async with session.get(url, timeout=25, headers=headers) as response:
                        if response.status == 200:
                            html_bytes = await response.read()
                            detected = from_bytes(html_bytes).best()
                            html_str = str(detected) if detected else html_bytes.decode("utf-8", errors="replace")
                            
                            await self.process_text(html_str, url)
                            await self.marcar_url_visitada(url)
                            return
                        else:
                            print(f"[BLOQUEIO HTTP {response.status}] O Recanto rejeitou o acesso a: {url}")
                            if response.status in [403, 404, 406, 503]:
                                return 
                            
                except Exception as e:
                    print(f"[ERRO REDE] Falha de conexão em {url}: {e}")
                    pass
                    
                if attempt < 2:
                    await asyncio.sleep((2 ** attempt) + random.uniform(0.5, 1.5))

    # ── Filtros e Limpeza ────────────────────────────────────────────────────
    def categorizar_tamanho(self, char_count: int) -> str:
        if 100 <= char_count <= 600: return "Curto"
        elif 601 <= char_count < 2501: return "Médio"
        elif 2501 <= char_count < 5000: return "Médio-Longo"
        elif 5000 <= char_count <= 30000: return "Longo"
        elif char_count > 30000: return "Muito Longo"
        return "Descartar"
    
    def is_literary_tone(self, texto: str) -> bool:
        texto_lower = texto.lower()
        pontuacao_resenha = sum(1 for term in REVIEW_TERMS if term in texto_lower)
        if pontuacao_resenha >= 2: return False
        return True

    def is_textual_article(self, texto: str) -> bool:
        paragrafos = [p for p in texto.split('\n') if p.strip()]
        if not paragrafos: return False
        return (sum(len(p.split()) for p in paragrafos) / len(paragrafos)) >= 10

    def limpar_residuos(self, texto: str) -> str:
        if not isinstance(texto, str): return texto
        texto = re.sub(r'https?://\S+|www\.\S+', '', texto)
        nav_patterns = [
            r'(?i)^publicidade\.?$', r'(?i)^compartilhar\.?$',
            r'(?i)^clique aqui\.?$', r'(?i)^leia (também|mais)\.?$',
            r'(?i)^veja (também|mais)\.?$', r'(?i)^saiba mais\.?$',
        ]
        linhas = [l for l in texto.split('\n') if not any(re.match(p, l.strip()) for p in nav_patterns)]
        texto = '\n'.join(linhas)
        inline = [r'\bleia (mais|também):?\b', r'\bveja (mais|também):?\b', r'\bclique (aqui|em)\b']
        texto = re.sub('|'.join(inline), '', texto, flags=re.IGNORECASE)
        texto = re.sub(r'[ \t]+', ' ', texto)
        texto = re.sub(r'\n{3,}', '\n\n', texto)
        texto = re.sub(r'^- .{1,40}$', '', texto, flags=re.MULTILINE)
        return texto.strip()

    async def process_text(self, html_str: str, original_url: str):
        data_pub = 'Desconhecida'
        soup = BeautifulSoup(html_str, 'html.parser')
        
        time_tag = soup.find('time', attrs={'itemprop': 'dateCreated'})
        if not time_tag: time_tag = soup.find('time') 
        
        if time_tag:
            datetime_attr = time_tag.get('datetime', '')
            ano_match = re.search(r'(\d{4})', datetime_attr)
            if ano_match:
                data_pub = ano_match.group(1)
        
        if data_pub == 'Desconhecida':
            ano_url = re.search(r'/([12][0-9]{3})/', original_url)
            if ano_url: data_pub = ano_url.group(1)

        extract_data = trafilatura.bare_extraction(
            html_str, url=original_url, target_language="pt",
            include_comments=False, include_tables=False, include_links=False
        )

        if not extract_data or not extract_data.text:
            print(f"[FANTASMA] Trafilatura não encontrou texto em: {original_url}") 
            return

        texto_limpo = self.limpar_residuos(extract_data.text)
        char_count  = len(texto_limpo)

        categoria = self.categorizar_tamanho(char_count)
        if categoria == "Descartar": return

        if any(term in texto_limpo.lower() for term in BLACKLIST_TERMS): return
        if any(re.search(p, texto_limpo, re.MULTILINE) for p in METADATA_PATTERNS): return
        if texto_limpo.count("\ufffd") > 10: return
        
        # [ALTERADO] Removido o filtro is_textual_article para o Recanto!
        # Poesias muitas vezes não passam no teste de "média de 10 palavras por parágrafo".
        # Se quiser manter poesias, não podemos forçar parágrafos longos.
        # Se você foca só em prosas, pode descomentar a linha abaixo.
        # if not self.is_textual_article(texto_limpo): return
        
        #if not self.is_literary_tone(texto_limpo): return

        try:
            if char_count > 200 and detect(texto_limpo) != 'pt': return
        except LangDetectException:
            return

        if data_pub != 'Desconhecida' and int(data_pub) > 2018:
            print(f"[DATA RECENTE] Ano {data_pub} > 2020. Descartado: {original_url}")
            return
            
        texto_hash = hashlib.md5(texto_limpo.encode('utf-8')).hexdigest()
        if texto_hash in self.seen_hashes: return
        self.seen_hashes.add(texto_hash)

        # ── [ALTERADO] Extração do Tema pela URL do Recanto ─────────────
        # Transforma "https://www.recantodasletras.com.br/poesias/12345" em "Poesias"
        caminho_url = urlparse(original_url).path.strip('/')
        partes = caminho_url.split('/')
        
        if len(partes) >= 1:
            tema_bruto = partes[0] # No Recanto a primeira pasta é a categoria
        else:
            tema_bruto = "Literatura Independente"
            
        tema_extraido = tema_bruto.replace('-', ' ').title()

        await self.save_record({
            "text_id":        str(uuid.uuid4()),
            "content":        texto_limpo,
            "label":          0,
            "broad_area":     "Literária",
            "specific_theme": tema_extraido,
            "char_count":     char_count,
            "word_count":     len(texto_limpo.split()),
            "size_category":  categoria,
            "creation_date":  data_pub,
            "source_url":     original_url,
            "source_name":    "recantodasletras.com.br", # [ALTERADO]
            "content_hash":   texto_hash,
        })
        print(f"[Salvo] {tema_extraido} | {data_pub} | {char_count} chars")

    # ── Orquestrador ─────────────────────────────────────────────────────────
    async def run(self, max_test_urls: Optional[int] = None):
        self.start_time = time.monotonic()
        await self.init_db()

        try:
            async with aiohttp.ClientSession() as session:
                
                # [NOVO] Executa a verificação ética (Content-Signals) antes de iniciar
                for domain in self.target_domains:
                    permitido = await self.verify_ai_content_signals(session, domain)
                    if not permitido:
                        print(f"Encerrando pipeline para proteger diretrizes de IA do domínio {domain}.")
                        return

                print("Fase 1: Discovery via Sitemaps (100% do Site)...")
                # [ALTERADO] Usando a função adaptada para o Recanto
                #domain_tasks = [self.discovery_recanto_profundo(session, d) for d in self.target_domains]
                #resultados = await asyncio.gather(*domain_tasks)

                all_links = []
                for domain in self.target_domains:
                    links_diretos = await self.get_all_sitemap_links(session, domain)
                    all_links.extend(links_diretos)

                print(f"Discovery concluído. Total: {len(all_links)} artigos encontrados.")

                if max_test_urls:
                    random.shuffle(all_links)
                    all_links = all_links[:max_test_urls]
                    print(f"[Modo Teste] Limitando a {max_test_urls} URLs.")

                print(f"\nFase 2: Processando links em lotes...")
                batch_size = 50 
                for i in range(0, len(all_links), batch_size):
                    if self.tempo_esgotado(): break

                    batch = all_links[i:i + batch_size]
                    await asyncio.gather(*[self.fetch_and_process_live(session, s) for s in batch])
                    await asyncio.sleep(1) 

        finally:
            await self.close_db()

        elapsed = time.monotonic() - self.start_time
        print(f"\n[Finalizado] {len(self.seen_hashes)} textos únicos no banco em {elapsed/60:.1f} minutos.")

if __name__ == "__main__":
    # [ALTERADO] Domínio alvo modificado
    dominios = [
        "www.recantodasletras.com.br"
    ]

    pipeline = LiveScraperPipeline(target_domains=dominios, max_concurrent_requests=200)
    
    # Deixe sem argumentos para baixar tudo
    asyncio.run(pipeline.run(max_test_urls=100))