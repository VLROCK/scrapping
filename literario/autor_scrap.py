import asyncio
import re
import aiohttp
import hashlib
from charset_normalizer import from_bytes
import uuid
import aiosqlite
import os
import random
import time
from urllib.parse import urlparse
from typing import List, Optional
import trafilatura
from langdetect import detect, DetectorFactory
from langdetect.lang_detect_exception import LangDetectException

DetectorFactory.seed = 0

DB_PATH = "data/dataset_literariro_autores.db"

# ── Filtros e Configurações ──────────────────────────────────────────────────
BLOCKED_URL_PATTERNS = [
    "/ao-vivo", "/index", "/categoria", "/tag/", "/autor/", 
    "/busca", "/search", "/page/", "/galeria", "/login", "/embed",     
    "/videos_e_fotos", "/video/", "/blog/", "/platb/", "/live/", "/fotos/", "/album/",

    "/resenha", "/critica", "/ensaio", "/artigo", "/opiniao", 
    "/entrevista", "/coluna", "/noticia", "/reportagem"
]

URL_CERTO = [
    ".html", "/publicacoes-artigos2/",
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

    # ── Fase 1: Descoberta Recursiva via Sitemap (100% do Site) ──────────────
    async def get_all_sitemap_links(self, session: aiohttp.ClientSession, domain: str) -> List[str]:
        """Varre o sitemap para encontrar todos os links publicados pelo site."""
        print(f"[Discovery] Procurando mapas do site para {domain}...")
        links_artigos = set()
        sitemaps_para_visitar = [
            f"https://{domain}/sitemap.xml",
            f"https://{domain}/sitemap_index.xml",
            f"https://{domain}/post-sitemap.xml"
        ]
        sitemaps_visitados = set()

        headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"}

        while sitemaps_para_visitar:
            url_sitemap = sitemaps_para_visitar.pop(0)
            if url_sitemap in sitemaps_visitados: continue
            sitemaps_visitados.add(url_sitemap)

            try:
                async with session.get(url_sitemap, timeout=20, headers=headers) as resp:
                    if resp.status == 200:
                        xml_data = await resp.text()
                        
                        # Extrai tudo que está dentro de <loc>
                        encontrados = re.findall(r'<loc>(.*?)</loc>', xml_data)
                        
                        for link in encontrados:
                            # Se for outro sitemap, adiciona na fila
                            link = link.replace('<![CDATA[', '').replace(']]>', '').strip()
                            if link.endswith('.xml'):
                                sitemaps_para_visitar.append(link)
                            # Se for uma página normal, adiciona nos artigos
                            elif domain in link and not any(lixo in link for lixo in BLOCKED_URL_PATTERNS):
                                if any(certo in link for certo in URL_CERTO):
                                    links_artigos.add(link)

                                
            except Exception:
                pass # Ignora sitemaps que derem erro e tenta o próximo

        print(f"  -> {domain}: {len(links_artigos)} links totais descobertos!")
        return list(links_artigos)
    
    async def discovery_autores_profundo(self, session: aiohttp.ClientSession, domain: str) -> List[str]:
        # 1. Pega as categorias pelo sitemap
        categorias = await self.get_all_sitemap_links(session, domain)
        print(f"[Discovery Profundo] Entrando em {len(categorias)} categorias para extrair os textos...")

        links_textos = set()
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
        }

        # 2. Entra em cada categoria e raspa os links
        for i, url_cat in enumerate(categorias):
            try:
                async with session.get(url_cat, timeout=15, headers=headers) as resp:
                    if resp.status == 200:
                        html_data = await resp.text()
                        
                        # Extrai todos os links que terminam em .html e pertencem ao site
                        # Aceita caminhos relativos (href="/...") ou absolutos (href="https://...")
                        encontrados = re.findall(r'href=["\'](?:https://autores\.com\.br)?(/[^"\']+\.html)["\']', html_data)
                        
                        for link in encontrados:
                            link_completo = f"https://autores.com.br{link}"
                            
                            # Evita pegar as próprias categorias de volta ou lixo
                            if not any(lixo in link_completo for lixo in BLOCKED_URL_PATTERNS):
                                links_textos.add(link_completo)
                                
            except Exception as e:
                pass # Ignora categorias quebradas e segue o jogo
            
            # Print de progresso a cada 20 categorias lidas
            if (i + 1) % 20 == 0:
                print(f"  -> Lidas {i+1}/{len(categorias)} categorias... Textos encontrados até agora: {len(links_textos)}")
                
            await asyncio.sleep(0.5) # Respiro vital para não derrubar o servidor do site

        # O sitemap também incluiu no nosso bolo as categorias, então removemos elas
        links_finais = links_textos - set(categorias)
        print(f"\n[Discovery Concluído] {len(links_finais)} textos REAIS encontrados para raspagem!")
        return list(links_finais)
    # ── Fase 2 e 3: Extração de Texto ao Vivo ────────────────────────────────
    # ── Fase 2 e 3: Extração de Texto ao Vivo ────────────────────────────────
    async def fetch_and_process_live(self, session: aiohttp.ClientSession, url: str):
        if self.tempo_esgotado():
            return

        # Headers mais complexos para simular um navegador humano real e driblar o Cloudflare
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
            "Accept-Language": "pt-BR,pt;q=0.9,en-US;q=0.8,en;q=0.7",
            "Upgrade-Insecure-Requests": "1",
            "Sec-Fetch-Dest": "document",
            "Sec-Fetch-Mode": "navigate",
            "Sec-Fetch-Site": "none",
            "Sec-Fetch-User": "?1"
        }

        print(f" -> Batendo na porta: {url}")

        async with self.semaphore:
            for attempt in range(3):
                try:
                    async with session.get(url, timeout=25, headers=headers) as response:
                        if response.status == 200:
                            html_bytes = await response.read()
                            detected = from_bytes(html_bytes).best()
                            html_str = str(detected) if detected else html_bytes.decode("utf-8", errors="replace")
                            
                            # Manda para o nosso Raio-X
                            await self.process_text(html_str, url)
                            await self.marcar_url_visitada(url)
                            return
                            
                        else:
                            # AGORA NENHUM ERRO ESCAPA! Vai imprimir qualquer código que não seja 200.
                            print(f"[HTTP {response.status}] O site bloqueou o robô: {url}")
                            
                            # Se for bloqueio forte, nem adianta tentar de novo
                            if response.status in [403, 404, 406, 503]:
                                return 
                            
                except Exception as e:
                    print(f"[Erro de Código/Rede] Falha em {url}: {str(e)}")
                    
                if attempt < 2:
                    await asyncio.sleep((2 ** attempt) + random.uniform(0.5, 1.5))
            
            print(f"[Desistência] Esgotou as 3 tentativas para: {url}")

    # ── Filtros e Limpeza (Mantidos do seu código) ───────────────────────────
    def categorizar_tamanho(self, char_count: int) -> str:
        if 100 <= char_count <= 600: return "Curto"
        elif 601 <= char_count < 2501: return "Médio"
        elif 2501 <= char_count < 5000: return "Médio-Longo"
        elif 5000 <= char_count <= 30000: return "Longo"
        elif char_count > 30000: return "Muito Longo"
        return "Descartar"
    
    def is_literary_tone(self, texto: str) -> bool:
        """Verifica se o texto parece uma resenha em vez de ficção."""
        texto_lower = texto.lower()
        
        # Conta quantos jargões de crítica literária aparecem no texto
        pontuacao_resenha = sum(1 for term in REVIEW_TERMS if term in texto_lower)
        
        # Se o texto usar 2 ou mais termos críticos, ele é classificado como resenha e bloqueado
        if pontuacao_resenha >= 2:
            return False
            
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
        return texto.strip()

    async def process_text(self, html_str: str, original_url: str):
        extract_data = trafilatura.bare_extraction(
            html_str, url=original_url, target_language="pt",
            include_comments=False, include_tables=False, include_links=False
        )

        # ← .text em vez de .get('text')
        if not extract_data or not extract_data.text:
            return

        texto_limpo = self.limpar_residuos(extract_data.text)
        char_count  = len(texto_limpo)

        categoria = self.categorizar_tamanho(char_count)
        if categoria == "Descartar":
            return

        if any(term in texto_limpo.lower() for term in BLACKLIST_TERMS):
            return
        if any(re.search(p, texto_limpo, re.MULTILINE) for p in METADATA_PATTERNS):
            return
        if texto_limpo.count("\ufffd") > 10:
            return
        if not self.is_textual_article(texto_limpo):   # ← usa o método
            return
        if not self.is_literary_tone(texto_limpo):
            return

        try:
            if char_count > 200 and detect(texto_limpo) != 'pt':
                return
        except LangDetectException:
            return

        texto_hash = hashlib.md5(texto_limpo.encode('utf-8')).hexdigest()
        if texto_hash in self.seen_hashes:
            return
        self.seen_hashes.add(texto_hash)

        # data_pub = (extract_data.date or '')[:4]
        if not data_pub or not data_pub.isdigit():
            data_pub = 'Desconhecida'
            
        ano_url = re.search(r'/([12][0-9]{3})/', original_url)
        if ano_url and data_pub == 'Desconhecida':
            data_pub = ano_url.group(1)

        # A Guilhotina: Se tiver data e for menor que 2020, morre aqui.
        if data_pub != 'Desconhecida' and int(data_pub) < 2020:
            # print(f"[Descartado] Anterior a 2020 ({data_pub}): {original_url}")
            return
            
        # Opcional: Se quiser ser 100% rigoroso e não aceitar textos sem data, 
        # descomente a linha abaixo:
        # if data_pub == 'Desconhecida': return

        # --- EXTRAÇÃO DO TEMA VIA URL ---
        # Transforma "https://autores.com.br/haicai.html" em "Haicai"
        caminho_url = urlparse(original_url).path.strip('/')
        partes = caminho_url.split('/')
        
        # O tema geralmente é a penúltima ou antepenúltima pasta
        if len(partes) >= 2:
            tema_bruto = partes[-2] # Pega a pasta antes do nome do arquivo
        else:
            tema_bruto = partes[0]
            
        # Limpa o tema (remove números tipo "97-relacionamento" para "Relacionamento")
        tema_extraido = re.sub(r'^\d+-', '', tema_bruto).replace('-', ' ').title()
        tema_extraido = tema_extraido.replace('.Html', '')
        
        # Se a URL for vazia ou estranha, coloca um padrão
        if not tema_extraido or len(tema_extraido) < 3:
            tema_extraido = "Literatura Independente"

        await self.save_record({
            "text_id":        str(uuid.uuid4()),
            "content":        texto_limpo,
            "label":          0,
            "broad_area":     "Literária",
            "specific_theme": tema_extraido, # <--- Injetando o tema capturado!
            "char_count":     char_count,
            "word_count":     len(texto_limpo.split()),
            "size_category":  categoria,
            "creation_date":  data_pub,
            "source_url":     original_url,
            "source_name":    "autores.com.br",
            "content_hash":   texto_hash,
        })
        print(f"[Salvo] {tema_extraido} | {data_pub} | {char_count} chars")

    # ── Orquestrador ─────────────────────────────────────────────────────────
    async def run(self, max_test_urls: Optional[int] = None):
        self.start_time = time.monotonic()
        await self.init_db()

        try:
            async with aiohttp.ClientSession() as session:
                print("Fase 1: Discovery via Sitemaps (100% do Site)...")
                domain_tasks = [self.discovery_autores_profundo(session, d) for d in self.target_domains]
                resultados = await asyncio.gather(*domain_tasks)

                all_links = []
                for links in resultados:
                    all_links.extend(links)

                print(f"Discovery concluído. Total: {len(all_links)} artigos encontrados.")

                if max_test_urls:
                    random.shuffle(all_links)
                    all_links = all_links[:max_test_urls]
                    print(f"[Modo Teste] Limitando a {max_test_urls} URLs.")

                print(f"\nFase 2: Processando links em lotes...")
                batch_size = 50 # Menor para não derrubar sites ao vivo
                for i in range(0, len(all_links), batch_size):
                    if self.tempo_esgotado(): break

                    batch = all_links[i:i + batch_size]
                    await asyncio.gather(*[self.fetch_and_process_live(session, s) for s in batch])
                    await asyncio.sleep(1) # Respiro para o servidor ao vivo

        finally:
            await self.close_db()

        elapsed = time.monotonic() - self.start_time
        print(f"\n[Finalizado] {len(self.seen_hashes)} textos únicos em {elapsed/60:.1f} minutos.")

if __name__ == "__main__":
    dominios = [
        "autores.com.br"
    ]

    pipeline = LiveScraperPipeline(target_domains=dominios, max_concurrent_requests=200)
    
    # Coloque max_test_urls=10 para um teste rápido, ou deixe None para baixar TUDO
    asyncio.run(pipeline.run(max_test_urls=200))