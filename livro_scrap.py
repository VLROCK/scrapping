import asyncio
import re
import aiohttp
import hashlib
import uuid
import aiosqlite
import os
import fitz
import io
import random
import pandas as pd
from urllib.parse import urlparse
from typing import List, Dict, Optional, Tuple
from charset_normalizer import from_bytes
import trafilatura
from langdetect import detect, DetectorFactory
from langdetect.lang_detect_exception import LangDetectException

DetectorFactory.seed = 0

# ── Caminhos ────────────────────────────────────────────────────────────────
DB_PATH        = "data/datasetlivros.db"
SCHEMA_PATH    = "schema.sql"
PPORTAL_DIR    = "pportal"          # pasta com os CSVs do PPortal

# ── Mapeamento de gênero BLPL → broad_area / specific_theme do dataset ──────
# Mapeia work_genre nativo do BLPL para os campos do schema da IC
BLPL_GENRE_MAP = {
    "Romance ou novela":                   ("Literária", "Romance"),
    "Poemas":                              ("Literária", "Poesia"),
    "Contos":                              ("Literária", "Contos"),
    "Teatro":                              ("Literária", "Drama"),
    "Crônicas ou artigos de jornal":       ("Literária", "Crônicas"),
    "Biografia":                           ("Literária", "Biografia"),
    "Memórias":                            ("Literária", "Biografia"),
    "Ensaio, estudo, polêmica":            ("Literária", "Ensaio"),
    "Literatura informativa e de viagens": ("Literária", "Narrativas de viagem"),
    "Crítica, teoria ou história literária": ("Literária", "Outros"),
    "Tradução":                            ("Literária", "Outros"),
    "Discurso, sermão ou oração":          ("Literária", "Outros"),
    "Outros":                              ("Literária", "Outros"),
    "Não identificado":                    ("Literária", "Outros"),
}

# Mapeamento de gênero Goodreads → broad_area / specific_theme
GOODREADS_GENRE_MAP = {
    "Classics":       ("Literária", "Romance"),
    "Literature":     ("Literária", "Outros"),
    "Romance":        ("Literária", "Romance"),
    "Short-stories":  ("Literária", "Contos"),
    "Poetry":         ("Literária", "Poesia"),
    "Fantasy":        ("Literária", "Fantasia"),
    "History":        ("Literária", "Literatura histórica"),
    "Drama":          ("Literária", "Drama"),
    "Novels":         ("Literária", "Romance"),
    "Philosophy":     ("Literária", "Literatura filosófica"),
    "Plays":          ("Literária", "Drama"),
    "Historical":     ("Literária", "Literatura histórica"),
    "Biography":      ("Literária", "Biografia"),
    "Mystery":        ("Literária", "Misterio"),
    "Thriller":       ("Literária", "Misterio"),
    "Horror":         ("Literária", "Terror"),
    "Science-fiction":("Literária", "Ficção científica"),
    "Adventure":      ("Literária", "Aventuras"),
    "Children":       ("Literária", "Literatura infantil"),
}


# ────────────────────────────────────────────────────────────────────────────

class BookScraperPipeline:

    def __init__(self, max_concurrent: int = 5, min_year: Optional[int] = None):
        self.semaphore        = asyncio.Semaphore(max_concurrent)
        self.seen_hashes: set = set()
        self.min_year         = min_year  # None = sem filtro de ano
        self.db               = None
        self.catalog: List[Dict] = []     # lista montada pelos CSVs

    # ── Banco de dados ───────────────────────────────────────────────────────

    async def init_db(self):
        os.makedirs("data", exist_ok=True)
        self.db = await aiosqlite.connect(DB_PATH)
        with open(SCHEMA_PATH, "r", encoding="utf-8") as f:
            await self.db.executescript(f.read())
        await self.db.commit()

        async with self.db.execute("SELECT content_hash FROM texts") as cur:
            self.seen_hashes = {r[0] for r in await cur.fetchall()}
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
            (record["text_id"], record["content"], record["label"],
             record["broad_area"], record["specific_theme"],
             record["char_count"], record["word_count"],
             record["size_category"], record["creation_date"],
             record["source_url"], record["source_name"],
             record["content_hash"]),
        )
        await self.db.commit()

    # ── Construção do catálogo ───────────────────────────────────────────────

    def build_catalog(self) -> List[Dict]:
        """
        Junta os CSVs do PPortal e retorna uma lista de dicts com:
        original_id, title, author, year, download_link,
        broad_area, specific_theme
        """
        print("[Catálogo] Carregando CSVs...")

        blpl     = pd.read_csv(f"{PPORTAL_DIR}/digital_library_BLPL.csv",
                               sep='\t', on_bad_lines='skip')
        prelim   = pd.read_csv(f"{PPORTAL_DIR}/digital_library_preliminary.csv",
                               sep='\t', on_bad_lines='skip')
        matching = pd.read_csv(f"{PPORTAL_DIR}/dl_goodreads_matching.csv",
                               sep='\t', on_bad_lines='skip')
        gwgenres = pd.read_csv(f"{PPORTAL_DIR}/goodreads_works_genres.csv",
                               sep='\t', on_bad_lines='skip')
        ggenres  = pd.read_csv(f"{PPORTAL_DIR}/goodreads_genres.csv",
                               sep='\t', on_bad_lines='skip')

        # 1. Filtrar só Obras Literárias disponíveis do BLPL
        blpl['year'] = pd.to_numeric(blpl['work_publication_year'], errors='coerce')
        mask = (blpl['file_available'] == True) & (blpl['work_category'] == 'Obra Literária')
        if self.min_year:
            mask &= blpl['year'] >= self.min_year
        blpl_ok = blpl[mask].copy()
        print(f"[Catálogo] Obras Literárias disponíveis: {len(blpl_ok)}")

        # 2. Adicionar link de download
        links = prelim[prelim['data_source'] == 'BLPL'][['original_id', 'download_link']]
        df = blpl_ok.merge(links, on='original_id', how='inner')
        print(f"[Catálogo] Com link de download: {len(df)}")

        # 3. Montar gênero Goodreads (gênero primário por livro)
        genre_full = gwgenres.merge(ggenres, on='genre_id')
        primary_genre = (genre_full.groupby('work_id')
                         .agg(gr_genre=('genre', lambda x: x.value_counts().index[0]))
                         .reset_index())
        df = df.merge(matching, on='original_id', how='left')
        df = df.merge(primary_genre, left_on='goodreads_id', right_on='work_id', how='left')

        # 4. Resolver broad_area e specific_theme
        def resolve_genre(row):
            # Prioridade 1: Goodreads
            if pd.notna(row.get('gr_genre')):
                mapped = GOODREADS_GENRE_MAP.get(row['gr_genre'])
                if mapped:
                    return mapped
            # Prioridade 2: work_genre nativo BLPL
            blpl_genre = row.get('work_genre', '')
            if pd.notna(blpl_genre):
                mapped = BLPL_GENRE_MAP.get(blpl_genre)
                if mapped:
                    return mapped
            return ("Literária", "Outros")

        df[['broad_area', 'specific_theme']] = df.apply(
            lambda r: pd.Series(resolve_genre(r)), axis=1
        )

        # 5. Converter para lista de dicts
        catalog = []
        for _, row in df.iterrows():
            catalog.append({
                "original_id":    row['original_id'],
                "title":          row.get('work_title', ''),
                "author":         row.get('work_authors', ''),
                "year":           str(int(row['year'])) if pd.notna(row['year']) else None,
                "download_link":  row['download_link'],
                "broad_area":     row['broad_area'],
                "specific_theme": row['specific_theme'],
            })

        print(f"[Catálogo] Total para processar: {len(catalog)}")

        # Mostrar distribuição de gêneros
        areas = pd.DataFrame(catalog)['specific_theme'].value_counts()
        print("[Catálogo] Distribuição de temas:")
        print(areas.head(10).to_string())

        return catalog

    # ── Download e extração ──────────────────────────────────────────────────

    def categorizar_tamanho(self, char_count: int) -> str:
        if 100 <= char_count <= 600:       return "Curto"
        elif 601 <= char_count < 2501:     return "Médio"
        elif 2501 <= char_count < 5000:    return "Médio-Longo"
        elif 5000 <= char_count <= 30000:  return "Longo"
        elif char_count > 30000:           return "Muito Longo"
        return "Descartar"

    def limpar_residuos(self, texto: str) -> str:
        if not isinstance(texto, str):
            return texto
        texto = re.sub(r'https?://\S+|www\.\S+', '', texto)
        nav_patterns = [
            r'(?i)^publicidade\.?$', r'(?i)^compartilhar\.?$',
            r'(?i)^clique aqui\.?$', r'(?i)^leia (também|mais)\.?$',
            r'(?i)^veja (também|mais)\.?$', r'(?i)^saiba mais\.?$',
        ]
        linhas = texto.split('\n')
        linhas = [l for l in linhas
                  if not any(re.match(p, l.strip()) for p in nav_patterns)]
        texto = '\n'.join(linhas)
        inline = [r'\bleia (mais|também):?\b', r'\bveja (mais|também):?\b',
                  r'\bclique (aqui|em)\b']
        texto = re.sub('|'.join(inline), '', texto, flags=re.IGNORECASE)
        texto = re.sub(r'[ \t]+', ' ', texto)
        texto = re.sub(r'\n{3,}', '\n\n', texto)
        return texto.strip()

    async def fetch_and_process(self, session: aiohttp.ClientSession, entry: Dict):
        url = entry['download_link']

        # Se for um link da UFSC, injeta o comando de download direto do arquivo
        if "literaturabrasileira.ufsc.br/documentos/?id=" in url:
            url = url.replace("?id=", "?action=download&id=")
            
        headers = {"User-Agent": "Mozilla/5.0 (compatible; IC-Research-Bot/1.0)"}

        async with self.semaphore:
            for attempt in range(3):
                try:
                    timeout = aiohttp.ClientTimeout(total=60) # PDFs demoram mais para baixar, aumentei para 60s
                    async with session.get(url, timeout=timeout, headers=headers) as response:
                        
                        if response.status == 200:
                            # 1. Checa o que o servidor enviou (PDF ou HTML?)
                            content_type = response.headers.get('Content-Type', '').lower()
                            file_bytes = await response.read()

                            if 'application/pdf' in content_type or url.endswith('.pdf'):
                                await self.process_pdf(file_bytes, entry)
                            else:
                                # Se não for PDF, assume que é HTML e usa o Trafilatura
                                detected = from_bytes(file_bytes).best()
                                html_str = (str(detected) if detected else file_bytes.decode('utf-8', errors='replace'))
                                await self.process_html(html_str, entry)
                            return

                        elif response.status in [429, 500, 502, 503, 504]:
                            print(f"[Aviso] HTTP {response.status} — tentativa {attempt+1}/3: {url}")
                        else:
                            return  # 404/403 — não vale tentar de novo

                except (aiohttp.ClientError, asyncio.TimeoutError):
                    print(f"[Aviso] Timeout — tentativa {attempt+1}/3: {url}")

                if attempt < 2:
                    await asyncio.sleep((2 ** attempt) + random.uniform(0.1, 1.0))

            print(f"[Erro] Falha definitiva: {url}")

    async def process_pdf(self, pdf_bytes: bytes, entry: Dict):
        """Abre o PDF na memória, extrai todo o texto e envia para fatiamento."""
        try:
            doc = fitz.open(stream=pdf_bytes, filetype="pdf")
            texto_completo = ""
            for page in doc:
                texto_completo += page.get_text() + "\n\n"
            doc.close()
            
            await self.fatiar_e_salvar(texto_completo, entry)
        except Exception as e:
            print(f"[Erro PDF] Não foi possível ler o PDF {entry['download_link']}: {e}")

    async def process_html(self, html_content: str, entry: Dict):
        """Se for uma página web, usa o trafilatura e envia para fatiamento."""
        texto_completo = trafilatura.extract(
            html_content,
            url=entry['download_link'],
            target_language="pt",
            include_comments=False,
            include_tables=False,
            favor_precision=True
        )
        if texto_completo:
            await self.fatiar_e_salvar(texto_completo, entry)

    async def fatiar_e_salvar(self, texto_completo: str, entry: Dict):
        """O Motor de Fatiamento (Chunking): Pega um livro inteiro e corta em pedaços perfeitos."""
        
        # Limpeza para arrumar quebras de linha estranhas comuns em PDFs
        texto_limpo = re.sub(r'(?<!\n)\n(?!\n)', ' ', texto_completo) 
        texto_limpo = self.limpar_residuos(texto_limpo)
        
        paragrafos = [p.strip() for p in texto_limpo.split('\n\n') if p.strip()]

        chunk_atual = ""
        chunks_salvos = 0

        for p in paragrafos:
            # Acumula parágrafos até o bloco ter cerca de 4000 caracteres (Categoria: Médio-Longo)
            if len(chunk_atual) < 4000:
                chunk_atual += p + "\n\n"
            else:
                sucesso = await self.avaliar_e_salvar_chunk(chunk_atual.strip(), entry)
                if sucesso: chunks_salvos += 1
                chunk_atual = p + "\n\n"

        # Salva o restinho do livro se for maior que 500 caracteres
        if len(chunk_atual) > 500:
            sucesso = await self.avaliar_e_salvar_chunk(chunk_atual.strip(), entry)
            if sucesso: chunks_salvos += 1
            
        if chunks_salvos > 0:
            print(f"[Livro Processado] '{entry.get('title','?')[:30]}...' rendeu {chunks_salvos} textos no DB.")


    async def avaliar_e_salvar_chunk(self, texto: str, entry: Dict) -> bool:
        """Aplica as regras finais em um pedaço do livro e salva no DB."""
        word_count = len(texto.split())
        char_count = len(texto)
        
        categoria = self.categorizar_tamanho(char_count)
        if categoria == "Descartar":
            return False

        # Filtro de idioma para garantir que o pedaço não é uma introdução em inglês/espanhol
        try:
            if char_count > 200 and detect(texto) not in ['pt']:
                return False
        except LangDetectException:
            return False

        texto_hash = hashlib.md5(texto.encode('utf-8')).hexdigest()
        if texto_hash in self.seen_hashes:
            return False
        self.seen_hashes.add(texto_hash)

        record = {
            "text_id":        str(uuid.uuid4()), # Cada chunk ganha um ID único
            "content":        texto,
            "label":          0,
            "broad_area":     entry['broad_area'],
            "specific_theme": entry['specific_theme'],
            "char_count":     char_count,
            "word_count":     word_count,
            "size_category":  categoria,
            "creation_date":  entry.get('year'),
            "source_url":     entry['download_link'],
            "source_name":    "PPORTAL/BLPL",
            "content_hash":   texto_hash,
        }
        await self.save_record(record)
        return True

   
    # ── Orquestrador ─────────────────────────────────────────────────────────

    async def run(self, max_books: Optional[int] = None, batch_size: int = 20):
        await self.init_db()

        try:
            self.catalog = self.build_catalog()

            if max_books:
                random.shuffle(self.catalog)
                self.catalog = self.catalog[:max_books]
                print(f"[Modo Teste] Limitando a {max_books} livros.")

            print(f"\n[Pipeline] Iniciando download de {len(self.catalog)} livros "
                  f"em lotes de {batch_size}...")

            async with aiohttp.ClientSession() as session:
                for i in range(0, len(self.catalog), batch_size):
                    batch = self.catalog[i:i + batch_size]
                    tasks = [self.fetch_and_process(session, e) for e in batch]
                    await asyncio.gather(*tasks)
                    print(f"[Progresso] Lote {i//batch_size + 1} concluído. "
                          f"Salvos até agora: {len(self.seen_hashes)}")
                    await asyncio.sleep(2)  # pausa entre lotes

        finally:
            await self.close_db()

        print(f"\n[Finalizado] Total de textos únicos salvos: {len(self.seen_hashes)}")


# ── Execução ──────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    pipeline = BookScraperPipeline(
        max_concurrent=5,
        min_year=None,   # None = sem filtro de ano (recomendado para BLPL)
                         # Use min_year=1950 se quiser só obras mais recentes
    )
    asyncio.run(pipeline.run(
        max_books=50,    # None = processar tudo; número = modo teste
        batch_size=20,
    ))