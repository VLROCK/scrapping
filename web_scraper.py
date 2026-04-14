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
from urllib.parse import urlparse
from typing import List, Dict, Optional
import trafilatura
from langdetect import detect, DetectorFactory
from langdetect.lang_detect_exception import LangDetectException

DetectorFactory.seed = 0

DB_PATH = "data/dataset.db"

BLOCKED_URL_PATTERNS = [
    "/ao-vivo", "/index", "/categoria", "/tag/",
    "/autor/", "/busca", "/search", "/page/", "/galeria", "/login","/embed",     
    "/videos_e_fotos","/video/", "/blog/", "/platb/", "/live/", "/fotos/", "/album/"
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


class WaybackScraperPipeline:

    def __init__(self, target_domains: List[str], max_concurrent_requests: int = 5):
        self.target_domains = target_domains
        self.semaphore      = asyncio.Semaphore(max_concurrent_requests)
        # Semáforo separado para o CDX — limita chamadas paralelas à API do Wayback
        self.cdx_semaphore  = asyncio.Semaphore(4)
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

    async def get_cdx_snapshots_trimestre(
        self,
        session: aiohttp.ClientSession,
        domain: str,
        ano: int,
        mes_inicio: str,
        mes_fim: str,
    ) -> List[Dict]:
        """Busca snapshots de um único trimestre. Chamado em paralelo."""
        cdx_url = "http://web.archive.org/cdx/search/cdx"
        params = {
            "url":    f"{domain}/*",
            "output": "json",
            "from":   f"{ano}{mes_inicio}",
            "to":     f"{ano}{mes_fim}",
            "fl":     "timestamp,original,statuscode,mimetype",
            "filter": ["statuscode:200", "mimetype:text/html"],
            "collapse": "urlkey",
            "limit":  "30",  # 8 × 4 trimestres × 12 anos = 384 por domínio
        }

        async with self.cdx_semaphore:
            for attempt in range(3):
                try:
                    timeout = aiohttp.ClientTimeout(total=30)
                    async with session.get(cdx_url, params=params,
                                           timeout=timeout) as response:
                        if response.status == 200:
                            text = await response.text()
                            if not text.strip():
                                return []
                            try:
                                data = json.loads(text)
                            except json.JSONDecodeError:
                                return []
                            if data and isinstance(data, list) and len(data) > 1:
                                keys = data[0]
                                return [dict(zip(keys, row)) for row in data[1:]]
                            return []

                except Exception:
                    if attempt < 2:
                        await asyncio.sleep((2 ** attempt) + random.uniform(0.1, 0.5))

        return []

    async def get_cdx_snapshots(
        self, session: aiohttp.ClientSession, domain: str
    ) -> List[Dict]:
        """
        Dispara todas as chamadas CDX do domínio em paralelo.
        12 anos × 4 trimestres = 48 tasks por domínio, limitadas pelo cdx_semaphore.
        Antes: sequencial com sleep = ~30min. Agora: ~1-2min.
        """
        trimestres = [("01", "03"), ("04", "06"), ("07", "09"), ("10", "12")]

        tasks = [
            self.get_cdx_snapshots_trimestre(session, domain, ano, m_ini, m_fim)
            for ano in range(2006, 2018)
            for m_ini, m_fim in trimestres
        ]

        resultados = await asyncio.gather(*tasks, return_exceptions=True)

        snapshots = []
        for r in resultados:
            if isinstance(r, list):
                snapshots.extend(r)

        print(f"  {domain}: {len(snapshots)} snapshots encontrados.")
        return snapshots

    # ── Download com retry ───────────────────────────────────────────────────

    async def fetch_and_process_html(
        self, session: aiohttp.ClientSession, snapshot: Dict
    ):
        if self.tempo_esgotado():
            return

        timestamp    = snapshot['timestamp']
        original_url = snapshot['original']

        if any(p in original_url for p in BLOCKED_URL_PATTERNS):
            return
        
        if await self.url_ja_visitada(original_url):
            return

        wm_url  = f"http://web.archive.org/web/{timestamp}id_/{original_url}"
        headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"}

        async with self.semaphore:
            for attempt in range(3):
                if self.tempo_esgotado():
                    return
                try:
                    timeout = aiohttp.ClientTimeout(total=25)  # reduzido de 30
                    async with session.get(wm_url, timeout=timeout,
                                           headers=headers) as response:
                        if response.status == 200:
                            html_bytes = await response.read()
                            try:
                                detected = from_bytes(html_bytes).best()
                                html_str = str(detected) if detected else None
                            except Exception:
                                html_str = None
                            if not html_str:
                                html_str = html_bytes.decode("utf-8", errors="replace")
                            await self.process_text(html_str, original_url, timestamp)
                            return

                        elif response.status in [429, 500, 502, 503, 504]:
                            print(f"[Aviso] HTTP {response.status} tentativa {attempt+1}/3")
                        else:
                            return  # 404/403 — não tenta de novo

                except (aiohttp.ClientError, asyncio.TimeoutError):
                    pass

                if attempt < 2:
                    await asyncio.sleep((2 ** attempt) + random.uniform(0.1, 1.0))

        await self.marcar_url_visitada(original_url)

    # ── Filtros de qualidade ─────────────────────────────────────────────────

    def categorizar_tamanho(self, char_count: int) -> str:
        if 100 <= char_count <= 600:       return "Curto"
        elif 601 <= char_count < 2501:     return "Médio"
        elif 2501 <= char_count < 5000:    return "Médio-Longo"
        elif 5000 <= char_count <= 30000:  return "Longo"
        elif char_count > 30000:           return "Muito Longo"
        return "Descartar"

    def is_textual_article(self, texto: str) -> bool:
        paragrafos = [p for p in texto.split('\n') if p.strip()]
        if not paragrafos:
            return False
        media = sum(len(p.split()) for p in paragrafos) / len(paragrafos)
        return media >= 10

    def has_metadata_pattern(self, texto: str) -> bool:
        return any(re.search(p, texto, re.MULTILINE) for p in METADATA_PATTERNS)

    def has_too_much_repetition(self, texto: str) -> bool:
        linhas = [l.strip() for l in texto.split('\n') if l.strip()]
        if not linhas:
            return True
        return len(set(linhas)) / len(linhas) < 0.6

    def has_blacklist_terms(self, texto: str) -> bool:
        return any(term in texto.lower() for term in BLACKLIST_TERMS)

    def has_mojibake(self, texto: str) -> bool:
        return texto.count("\ufffd") > 10

    def limpar_residuos(self, texto: str) -> str:
        if not isinstance(texto, str):
            return texto
        texto = re.sub(r'https?://\S+|www\.\S+', '', texto)
        nav_patterns = [
            r'(?i)^publicidade\.?$', r'(?i)^compartilhar\.?$',
            r'(?i)^clique aqui\.?$', r'(?i)^leia (também|mais)\.?$',
            r'(?i)^veja (também|mais)\.?$', r'(?i)^saiba mais\.?$',
        ]
        linhas = [l for l in texto.split('\n')
                  if not any(re.match(p, l.strip()) for p in nav_patterns)]
        texto = '\n'.join(linhas)
        inline = [r'\bleia (mais|também):?\b', r'\bveja (mais|também):?\b',
                  r'\bclique (aqui|em)\b']
        texto = re.sub('|'.join(inline), '', texto, flags=re.IGNORECASE)
        texto = re.sub(r'[ \t]+', ' ', texto)
        texto = re.sub(r'\n{3,}', '\n\n', texto)
        return texto.strip()

    # ── Processamento principal ──────────────────────────────────────────────

    async def process_text(self, html_str: str, original_url: str, timestamp: str):
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

        # Filtros em ordem do mais barato ao mais caro
        if self.has_mojibake(texto_limpo):            return
        if self.has_metadata_pattern(texto_limpo):   return
        if self.has_blacklist_terms(texto_limpo):     return
        if self.has_too_much_repetition(texto_limpo): return

        linhas_totais   = texto_limpo.split('\n')
        linhas_conteudo = [l for l in linhas_totais if l.strip()]
        if not linhas_conteudo:
            return

        if 1 - len(linhas_conteudo) / len(linhas_totais) > 0.4:
            return

        word_count = len(texto_limpo.split())
        if word_count / len(linhas_conteudo) < 4:
            return

        if len(texto_limpo) / (len(html_str) + 1) < 0.02:
            return

        if not self.is_textual_article(texto_limpo):
            return

        char_count = len(texto_limpo)
        categoria  = self.categorizar_tamanho(char_count)
        if categoria == "Descartar":
            return

        try:
            if char_count > 200 and detect(texto_limpo) not in ['pt']:
                return
        except LangDetectException:
            return

        texto_hash = hashlib.md5(texto_limpo.encode('utf-8')).hexdigest()
        if texto_hash in self.seen_hashes:
            return
        self.seen_hashes.add(texto_hash)

        await self.save_record({
            "text_id":        str(uuid.uuid4()),
            "content":        texto_limpo,
            "label":          0,
            "broad_area":     "Jornalística",
            "specific_theme": None,
            "char_count":     char_count,
            "word_count":     word_count,
            "size_category":  categoria,
            "creation_date":  timestamp[:4],
            "source_url":     original_url,
            "source_name":    urlparse(original_url).hostname,
            "content_hash":   texto_hash,
        })
        print(f"[Salvo] {original_url} ({char_count} chars)")

    # ── Orquestrador ─────────────────────────────────────────────────────────

    async def run(self, max_test_urls: Optional[int] = None):
        """Orquestrador do pipeline."""
        self.start_time = time.monotonic()
        await self.init_db()

        try:
            async with aiohttp.ClientSession() as session:

                # Fase 1: discovery de TODOS os domínios em paralelo
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
                print(f"Discovery concluído em {elapsed:.0f}s. "
                      f"Total: {len(all_snapshots)} snapshots.")

                if max_test_urls:
                    random.shuffle(all_snapshots)
                    all_snapshots = all_snapshots[:max_test_urls]
                    print(f"[Modo Teste] Limitando a {max_test_urls} URLs.")

                # Fase 2/3: download e processamento em lotes
                print(f"\nFase 2/3: Processando {len(all_snapshots)} links...")
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
        print(f"\n[Finalizado] {len(self.seen_hashes)} textos únicos "
              f"em {elapsed/60:.1f} minutos.")


# ── Execução ──────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    dominios = [
        "g1.globo.com/ciencia-e-saude",
        "bbc.com/portuguese",
        "noticias.uol.com.br",
        "estadao.com.br",
        "super.abril.com.br",
        "revistagalileu.globo.com",
        "exame.com",
        "folha.uol.com.br",
        "oglobo.globo.com",
        "correiobraziliense.com.br",
        "nexojornal.com.br",
        "brasil.elpais.com",
    ]

    pipeline = WaybackScraperPipeline(
        target_domains=dominios,
        max_concurrent_requests=5,
    )
    asyncio.run(pipeline.run())