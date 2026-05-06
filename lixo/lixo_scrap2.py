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
import time
import sqlite3
from urllib.parse import urlparse
from typing import List, Dict, Optional
import trafilatura
from langdetect import detect, DetectorFactory
from langdetect.lang_detect_exception import LangDetectException

DetectorFactory.seed = 0

# Você pode salvar no mesmo banco de lixo anterior para acumular, 
# ou criar um específico. Vamos usar um nome genérico para o lixo.
DB_PATH = "data/dataset_lixo.db"

BLOCKED_URL_PATTERNS = [
    "/ao-vivo", "/index", "/categoria", "/tag/",
    "/autor/", "/busca", "/search", "/page/", "/galeria", "/login","/embed",     
    "/videos_e_fotos","/video/", "/blog/", "/platb/", "/live/", "/fotos/", "/album/", "/enquete/", "/blogs/",
]

BLACKLIST_TERMS = [
    "cookies", "termos de uso", "política de privacidade",
    "assine", "newsletter", "cadastre-se", "media player"
]

METADATA_PATTERNS = [
    r'^\s*-\s*\w+:',
    r'timestamp:',
    r'totalImagens:',
    r'fotoInicial:',
    r'imagePath:',
    r'ordem:',
]

# Tempo máximo total — 5h para folgar no limite de 6h do GitHub Actions
MAX_RUNTIME_SECONDS = 5 * 60 * 60


class WaybackScraperInformationalTrashPipeline:

    def __init__(self, target_domains: List[str], max_concurrent_requests: int = 5):
        self.target_domains = target_domains
        self.semaphore      = asyncio.Semaphore(max_concurrent_requests)
        self.cdx_semaphore  = asyncio.Semaphore(4)
        self.seen_hashes: set = set()
        self.start_time: float = 0.0

    def tempo_esgotado(self) -> bool:
        return (time.monotonic() - self.start_time) > MAX_RUNTIME_SECONDS

    # ── Banco de dados (C/ Proteção contra Corrupção Github Actions) ────────

    async def init_db(self):
        os.makedirs("data", exist_ok=True)
        
        # 1. Tenta conectar e ler o banco. Se der erro de corrupção, apaga e recria.
        try:
            self.db = await aiosqlite.connect(DB_PATH)
            await self.db.execute("PRAGMA integrity_check;")
        except sqlite3.DatabaseError:
            print(f"⚠️ [ALERTA CRÍTICO] Banco {DB_PATH} corrompido no Github Actions Cache!")
            print("Limpando arquivo quebrado e recriando do zero...")
            if hasattr(self, 'db') and self.db:
                await self.db.close()
            if os.path.exists(DB_PATH):
                os.remove(DB_PATH)
            self.db = await aiosqlite.connect(DB_PATH)

        # 2. Continua a execução normal
        try:
            with open("schema.sql", "r", encoding="utf-8") as f:
                await self.db.executescript(f.read())
            await self.db.commit()

            async with self.db.execute("SELECT content_hash FROM texts") as cursor:
                rows = await cursor.fetchall()
            self.seen_hashes = {row[0] for row in rows}
            print(f"[DB LIXO] {len(self.seen_hashes)} textos de lixo já existentes carregados.")
        
        except Exception as e:
            print(f"❌ [ERRO GRAVE] Falha ao criar schema ou ler memória: {e}")
            raise

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

    # ── CDX Discovery (paralelo) ─────────────────────────────────────────────

    async def url_ja_visitada(self, url: str) -> bool:
        url_hash = hashlib.md5(url.encode()).hexdigest()
        async with self.db.execute(
            "SELECT 1 FROM visited_urls WHERE url_hash = ?", (url_hash,)
        ) as cur:
            return await cur.fetchone() is not None

    async def marcar_url_visitada(self, url: str):
        url_hash = hashlib.md5(url.encode()).hexdigest()
        await self.db.execute(
            "INSERT OR IGNORE INTO visited_urls (url_hash, visited_at) VALUES (?, datetime('now'))",
            (url_hash,)
        )
        await self.db.commit()

    async def get_cdx_snapshots_trimestre(self, session: aiohttp.ClientSession, domain: str, ano: int, mes_inicio: str, mes_fim: str) -> List[Dict]:
        cdx_url = "http://web.archive.org/cdx/search/cdx"
        params = {
            "url":      f"{domain}/*",
            "output": "json",
            "from":   f"{ano}{mes_inicio}",
            "to":     f"{ano}{mes_fim}",
            "fl":     "timestamp,original,statuscode,mimetype",
            "filter": ["statuscode:200", "mimetype:text/html"],
            "collapse": "urlkey",
            "limit":  "30",  
        }

        async with self.cdx_semaphore:
            for attempt in range(3):
                try:
                    timeout = aiohttp.ClientTimeout(total=30)
                    async with session.get(cdx_url, params=params, timeout=timeout) as response:
                        if response.status == 200:
                            text = await response.text()
                            if not text.strip(): return []
                            try: data = json.loads(text)
                            except json.JSONDecodeError: return []
                            if data and isinstance(data, list) and len(data) > 1:
                                keys = data[0]
                                return [dict(zip(keys, row)) for row in data[1:]]
                            return []
                except Exception:
                    if attempt < 2: await asyncio.sleep((2 ** attempt) + random.uniform(0.1, 0.5))
        return []

    async def get_cdx_snapshots(self, session: aiohttp.ClientSession, domain: str) -> List[Dict]:
        trimestres = [("03", "03"), ("06", "06"), ("09", "09"), ("12", "12")]
        tasks = [
            self.get_cdx_snapshots_trimestre(session, domain, ano, m_ini, m_fim)
            for ano in range(2006, 2018)
            for m_ini, m_fim in trimestres
        ]
        resultados = await asyncio.gather(*tasks, return_exceptions=True)
        snapshots = []
        for r in resultados:
            if isinstance(r, list): snapshots.extend(r)
        print(f"  {domain}: {len(snapshots)} snapshots encontrados.")
        return snapshots

    # ── Download com retry ───────────────────────────────────────────────────

    async def fetch_and_process_html(self, session: aiohttp.ClientSession, snapshot: Dict):
        if self.tempo_esgotado(): return

        timestamp    = snapshot['timestamp']
        original_url = snapshot['original']

        # [LÓGICA INVERTIDA] Removemos o guardrail. Nós queremos as URLs bloqueadas!
        # if any(p in original_url for p in BLOCKED_URL_PATTERNS): return
        
        if await self.url_ja_visitada(original_url): return

        wm_url  = f"http://web.archive.org/web/{timestamp}id_/{original_url}"
        headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"}

        async with self.semaphore:
            for attempt in range(3):
                if self.tempo_esgotado(): return
                try:
                    timeout = aiohttp.ClientTimeout(total=25) 
                    async with session.get(wm_url, timeout=timeout, headers=headers) as response:
                        if response.status == 200:
                            html_bytes = await response.read()
                            try:
                                detected = from_bytes(html_bytes).best()
                                html_str = str(detected) if detected else None
                            except Exception: html_str = None
                            
                            if not html_str:
                                html_str = html_bytes.decode("utf-8", errors="replace")
                                
                            await self.process_text(html_str, original_url, timestamp)
                            return
                        elif response.status in [429, 500, 502, 503, 504]:
                            pass 
                        else: return 
                except (aiohttp.ClientError, asyncio.TimeoutError): pass
                if attempt < 2: await asyncio.sleep((2 ** attempt) + random.uniform(0.1, 1.0))
        await self.marcar_url_visitada(original_url)

    # ── Filtros Analíticos ───────────────────────────────────────────────────

    def categorizar_tamanho(self, char_count: int) -> str:
        if 100 <= char_count <= 600:       return "Curto"
        elif 601 <= char_count < 2501:     return "Médio"
        elif 2501 <= char_count < 5000:    return "Médio-Longo"
        elif 5000 <= char_count <= 30000:  return "Longo"
        elif char_count > 30000:           return "Muito Longo"
        return "Descartar"

    def is_textual_article(self, texto: str) -> bool:
        paragrafos = [p for p in texto.split('\n') if p.strip()]
        if len(paragrafos) < 6: return False
        media = sum(len(p.split()) for p in paragrafos) / len(paragrafos)
        return media >= 4

    def has_metadata_pattern(self, texto: str) -> bool:
        return any(re.search(p, texto, re.MULTILINE) for p in METADATA_PATTERNS)

    def has_too_much_repetition(self, texto: str) -> bool:
        linhas = [l.strip() for l in texto.split('\n') if l.strip()]
        if not linhas: return True
        return len(set(linhas)) / len(linhas) < 0.6

    def has_blacklist_terms(self, texto: str) -> bool:
        return any(term in texto.lower() for term in BLACKLIST_TERMS)

    def has_mojibake(self, texto: str) -> bool:
        return texto.count("\ufffd") > 10

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

    # ── [LÓGICA INVERTIDA] O Avaliador de Lixo ───────────────────────────────

    async def process_text(self, html_str: str, original_url: str, timestamp: str):
        texto_limpo = trafilatura.extract(
            html_str, url=original_url, target_language="pt",
            include_comments=False, include_tables=False, favor_precision=True,
            include_images=False, include_links=False,
        )
        
        # Ignoramos completamente vazios (não nos ajudam a treinar o modelo)
        if not texto_limpo or len(texto_limpo.strip()) < 50: 
            return

        texto_limpo = self.limpar_residuos(texto_limpo)
        motivo_lixo = None

        # 1. Armadilha de URL
        if any(p in original_url for p in BLOCKED_URL_PATTERNS):
            motivo_lixo = "URL Bloqueada (Menus/Busca)"
        # 2. Armadilha de Conteúdo Defeituoso
        elif self.has_mojibake(texto_limpo):
            motivo_lixo = "Mojibake/Encoding Quebrado"
        elif self.has_metadata_pattern(texto_limpo):
            motivo_lixo = "Vazamento de Metadados"
        elif self.has_blacklist_terms(texto_limpo):
            motivo_lixo = "Termos Proibidos (Cookies/Privacidade)"
        elif self.has_too_much_repetition(texto_limpo):
            motivo_lixo = "Excesso de Repetição"

        # 3. Armadilha de Estrutura Não-Natural
        if not motivo_lixo:
            linhas_totais   = texto_limpo.split('\n')
            linhas_conteudo = [l for l in linhas_totais if l.strip()]
            word_count = len(texto_limpo.split())

            if not linhas_conteudo: return
            elif 1 - len(linhas_conteudo) / len(linhas_totais) > 0.4:
                motivo_lixo = "Muitas linhas vazias"
            elif word_count / len(linhas_conteudo) < 4:
                motivo_lixo = "Poucas palavras por linha (Falso tutorial)"
            elif len(texto_limpo) / (len(html_str) + 1) < 0.02:
                motivo_lixo = "Baixo ratio Texto/HTML"
            elif not self.is_textual_article(texto_limpo):
                motivo_lixo = "Formato Fragmentado (Listas curtas)"

        # 4. Armadilha de Tamanho e Idioma
        char_count = len(texto_limpo)
        categoria  = self.categorizar_tamanho(char_count)

        if not motivo_lixo:
            if categoria == "Descartar":
                motivo_lixo = "Tamanho Extremo (Muito Curto/Longo)"
            else:
                try:
                    if char_count > 200 and detect(texto_limpo) not in ['pt']:
                        motivo_lixo = "Idioma Estrangeiro"
                except LangDetectException:
                    motivo_lixo = "Falha na Detecção de Idioma"

        # Se não tiver motivo para ser lixo, significa que o texto é um ÓTIMO tutorial.
        # Descartamos, pois estamos focados em lixo!
        if not motivo_lixo:
            return

        texto_hash = hashlib.md5(texto_limpo.encode('utf-8')).hexdigest()
        if texto_hash in self.seen_hashes:
            return
        self.seen_hashes.add(texto_hash)

        await self.save_record({
            "text_id":        str(uuid.uuid4()),
            "content":        texto_limpo,
            "label":          1, # <--- MARCADO COMO LIXO!
            "broad_area":     "Ruído/Lixo (Informacional)",
            "specific_theme": motivo_lixo, # O motivo gravado como theme
            "char_count":     char_count,
            "word_count":     len(texto_limpo.split()),
            "size_category":  categoria,
            "creation_date":  timestamp[:4],
            "source_url":     original_url,
            "source_name":    urlparse(original_url).hostname,
            "content_hash":   texto_hash,
        })
        print(f"🗑️ [LIXO SALVO] {motivo_lixo} | {original_url}")

    # ── Orquestrador ─────────────────────────────────────────────────────────

    async def run(self, max_test_urls: Optional[int] = None):
        """Orquestrador do pipeline."""
        self.start_time = time.monotonic()
        await self.init_db()

        try:
            async with aiohttp.ClientSession() as session:
                print("Fase 1: Discovery CDX (paralelo por domínio)...")
                domain_tasks = [
                    self.get_cdx_snapshots(session, d)
                    for d in self.target_domains
                ]
                resultados = await asyncio.gather(*domain_tasks)

                all_snapshots = []
                for snaps in resultados:
                    all_snapshots.extend(snaps)

                elapsed = time.monotonic() - self.start_time
                print(f"Discovery concluído em {elapsed:.0f}s. Total: {len(all_snapshots)} snapshots.")

                if max_test_urls:
                    random.shuffle(all_snapshots)
                    all_snapshots = all_snapshots[:max_test_urls]
                    print(f"[Modo Teste] Limitando a {max_test_urls} URLs.")

                print(f"\nFase 2/3: Capturando lixo em {len(all_snapshots)} links...")
                batch_size = 100
                for i in range(0, len(all_snapshots), batch_size):
                    if self.tempo_esgotado():
                        print("[Aviso] Tempo limite atingido — encerrando com segurança.")
                        break

                    batch = all_snapshots[i:i + batch_size]
                    await asyncio.gather(
                        *[self.fetch_and_process_html(session, s) for s in batch]
                    )

                    elapsed = time.monotonic() - self.start_time
                    print(f"[Progresso] Lote {i//batch_size + 1} | "
                          f"Salvos: {len(self.seen_hashes)} | "
                          f"Tempo: {elapsed/60:.1f}min")
                    await asyncio.sleep(1)

        finally:
            await self.close_db()

        elapsed = time.monotonic() - self.start_time
        print(f"\n[Finalizado] {len(self.seen_hashes)} fragmentos de lixo "
              f"capturados em {elapsed/60:.1f} minutos.")


# ── Execução ──────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    dominios = [
        "tudogostoso.com.br",
        "panelinha.com.br",
        "receitas.ig.com.br",
        "techtudo.com.br",
        "tecmundo.com.br",
        "canaltech.com.br",
        "mdsaude.com",
        "tuasaude.com",
        "dicasdemulher.com.br",
        "catracalivre.com.br",
        "fazfacil.com.br",      
        "tuacasa.com.br",       
        "vivadecora.com.br",    
        "leroymerlin.com.br",   
        "alura.com.br",         
        "devmedia.com.br",      
        "diolinux.com.br",      
        "macoratti.net",        
        "treinaweb.com.br",
        "cifraclub.com.br",         
        "descomplicandoamusica.com", 
        "fotografia-dg.com",        
        "resumofotografico.com",    
        "clubedodesign.com",        
        "designerd.com.br",         
        "esbocandoideias.com",      
        "revistaartesanato.com.br"
    ]

    pipeline = WaybackScraperInformationalTrashPipeline(
        target_domains=dominios,
        max_concurrent_requests=5,
    )
    asyncio.run(pipeline.run())