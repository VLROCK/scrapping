import asyncio
import aiohttp
import uuid
import hashlib
import aiosqlite
import os
import zipfile
import io
import re
from bs4 import BeautifulSoup
import trafilatura
from langdetect import detect, DetectorFactory

DetectorFactory.seed = 0

# ── Configurações ────────────────────────────────────────────────────────────
DB_PATH = "data/dataset_livro.db"
SCHEMA_PATH = "schema.sql"
BASE_SITEMAP = "https://projectoadamastor.org/post-sitemap.xml" # Mapa oculto com todos os livros

class AdamastorScraperPipeline:
    def __init__(self, max_concurrent: int = 5):
        self.semaphore = asyncio.Semaphore(max_concurrent)
        self.seen_hashes = set()
        self.db = None

    # ── Infraestrutura do Banco ──────────────────────────────────────────────
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
        if self.db: await self.db.close()

    async def save_record(self, record: dict):
        await self.db.execute(
            """INSERT OR IGNORE INTO texts
               (text_id, content, label, broad_area, specific_theme,
                char_count, word_count, size_category, source_url, source_name, content_hash)
               VALUES (?,?,?,?,?,?,?,?,?,?,?)""",
            (record["text_id"], record["content"], record["label"],
             record["broad_area"], record["specific_theme"], record["char_count"],
             record["word_count"], record["size_category"], record["source_url"],
             record["source_name"], record["content_hash"])
        )
        await self.db.commit()

    # ── Lógica de Fatiamento (As regras da Tabela 2) ───────────────────────
    def categorizar_tamanho(self, char_count: int) -> str:
        if 100 <= char_count <= 600: return "Curto"
        elif 601 <= char_count <= 2500: return "Médio"
        elif 2501 <= char_count <= 5000: return "Médio-Longo"
        elif 5000 < char_count <= 30000: return "Longo"
        return "Descartar"

    # ── Fase 1: Descoberta (Sitemap) ─────────────────────────────────────────
    async def get_book_pages(self, session: aiohttp.ClientSession, limite_descoberta: int = None) -> list:
        """Entra na página pública de catálogo e puxa os links como um humano faria."""
        print("[Descoberta] Acessando a vitrine do catálogo do Projecto Adamastor...")
        links_livros = set() # Usamos 'set' para não pegar o mesmo livro duplicado
        
        # O disfarce perfeito: finge ser o Google Chrome no Windows
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
        }
        
        try:
            # Acessa a página principal onde os livros estão listados
            async with session.get("https://projectoadamastor.org/catalogo/", timeout=20, headers=headers) as resp:
                if resp.status == 200:
                    html = await resp.text()
                    soup = BeautifulSoup(html, 'html.parser')
                    
                    # Pega todos os links (tags <a>) da página
                    for a_tag in soup.find_all('a', href=True):
                        url = a_tag['href']
                        
                        # Um livro no Adamastor tem o link "https://projectoadamastor.org/nome-do-livro/"
                        if url.startswith("https://projectoadamastor.org/") and url != "https://projectoadamastor.org/":
                            
                            # Ignora os links que são apenas menus, categorias ou autores
                            lixo = ['/catalogo/', '/autor/', '/categoria/', '/tag/', '/sobre/', '/contactos/']
                            if not any(palavra in url for palavra in lixo):
                                links_livros.add(url)
                                
                                # A nossa trava de limite
                                if limite_descoberta and len(links_livros) >= limite_descoberta:
                                    break
                else:
                    print(f"[Erro] O servidor retornou o código {resp.status} na página do catálogo.")

        except Exception as e:
            print(f"[Erro Descoberta] Falha ao ler o catálogo: {e}")
            
        lista_final = list(links_livros)
        print(f"[Descoberta] {len(lista_final)} páginas de livros encontradas no catálogo!")
        return lista_final

    # ── Fase 2 e 3: Visitar Página e Baixar EPUB ─────────────────────────────
    async def process_book_page(self, session: aiohttp.ClientSession, page_url: str):
        """Entra na página do livro, acha o botão do EPUB e faz o download."""
        async with self.semaphore:
            try:
                # 1. Visita a página do livro
                async with session.get(page_url, timeout=30) as resp:
                    if resp.status != 200: 
                        print(f"[Erro] Página inacessível ({resp.status}): {page_url}")
                        return
                        
                    html = await resp.text()
                    soup = BeautifulSoup(html, 'html.parser')

                    # 2. Procura a tag <a> de forma muito mais agressiva
                    epub_link = None
                    for a_tag in soup.find_all('a', href=True):
                        href = a_tag['href']
                        texto_botao = a_tag.get_text().upper()
                        
                        # Verifica se o link contém .epub OU se o botão diz "EPUB"
                        if '.epub' in href.lower() or 'EPUB' in texto_botao:
                            epub_link = href
                            break
                    
                    if not epub_link:
                        print(f"[Aviso] Nenhum link EPUB encontrado em: {page_url}")
                        return

                    # Se o link for relativo (ex: /wp-content/...), transforma em absoluto
                    if epub_link.startswith('/'):
                        epub_link = "https://projectoadamastor.org" + epub_link

                    print(f" -> Baixando: {epub_link.split('/')[-1]}")

                    # 3. Baixa o arquivo EPUB para a memória
                    async with session.get(epub_link, timeout=45) as epub_resp:
                        if epub_resp.status == 200:
                            epub_bytes = await epub_resp.read()
                            # 4. Envia para fatiamento!
                            await self.process_epub_bytes(epub_bytes, page_url)
                        else:
                            print(f"[Erro] O arquivo EPUB retornou código {epub_resp.status}: {epub_link}")
                            
            except Exception as e:
                print(f"[Erro na Página] {page_url} -> {e}")

    # ── Fase 4: O "Descompactador" de EPUB ───────────────────────────────────
    async def process_epub_bytes(self, epub_bytes: bytes, original_page_url: str):
        """Abre o EPUB como ZIP, tira o HTML puro e fatia."""
        try:
            texto_completo = ""
            with zipfile.ZipFile(io.BytesIO(epub_bytes)) as archive:
                for item in archive.namelist():
                    # Os capítulos literários ficam em arquivos html/xhtml no EPUB
                    if item.endswith('.html') or item.endswith('.xhtml'):
                        html_content = archive.read(item)
                        
                        # Trafilatura arranca o HTML e deixa só o português polido
                        texto_capitulo = trafilatura.extract(
                            html_content,
                            target_language="pt",
                            include_comments=False,
                            include_tables=False,
                            favor_precision=True
                        )
                        if texto_capitulo:
                            texto_completo += texto_capitulo + "\n\n"

            if texto_completo.strip():
                await self.fatiar_e_salvar(texto_completo, original_page_url)
                
        except Exception as e:
            print(f"[Erro EPUB] Falha ao processar o EPUB de {original_page_url}: {e}")

    # ── Fase 5: Chunking (Fatiamento) ────────────────────────────────────────
    async def fatiar_e_salvar(self, texto_completo: str, source_url: str):
        # Limpeza básica de quebras de linha múltiplas
        texto_limpo = re.sub(r'\n{3,}', '\n\n', texto_completo).strip()
        paragrafos = [p.strip() for p in texto_limpo.split('\n\n') if p.strip()]

        chunk_atual = ""
        chunks_salvos = 0

        for p in paragrafos:
            # Acumula até ~3500 chars para garantir blocos robustos
            if len(chunk_atual) < 3500:
                chunk_atual += p + "\n\n"
            else:
                if await self.avaliar_e_salvar_chunk(chunk_atual.strip(), source_url):
                    chunks_salvos += 1
                chunk_atual = p + "\n\n"

        if len(chunk_atual) > 500:
            if await self.avaliar_e_salvar_chunk(chunk_atual.strip(), source_url):
                chunks_salvos += 1
                
        if chunks_salvos > 0:
            nome_livro = source_url.split('/')[-2].replace('-', ' ').title()
            print(f"[SUCESSO] '{nome_livro}' rendeu {chunks_salvos} amostras perfeitas!")

    async def avaliar_e_salvar_chunk(self, texto: str, source_url: str) -> bool:
        char_count = len(texto)
        categoria = self.categorizar_tamanho(char_count)
        if categoria == "Descartar": return False

        texto_hash = hashlib.md5(texto.encode('utf-8')).hexdigest()
        if texto_hash in self.seen_hashes: return False
        
        try:
            if detect(texto) != 'pt': return False
        except: return False

        self.seen_hashes.add(texto_hash)

        record = {
            "text_id": str(uuid.uuid4()),
            "content": texto,
            "label": 0,
            "broad_area": "Literária",     # Tudo do Adamastor é literatura clássica
            "specific_theme": "Ficção/Contos/Poesia",
            "char_count": char_count,
            "word_count": len(texto.split()),
            "size_category": categoria,
            "source_url": source_url,
            "source_name": "projectoadamastor.org",
            "content_hash": texto_hash
        }
        
        await self.save_record(record)
        return True

    # ── Orquestrador ─────────────────────────────────────────────────────────
    async def run(self, max_books: int = None):
        await self.init_db()
        try:
            async with aiohttp.ClientSession() as session:
                paginas = await self.get_book_pages(session)

                if max_books:
                    import random
                    random.shuffle(paginas) # Embaralha para pegar 10 livros aleatórios
                    paginas = paginas[:max_books]
                    print(f"\n[MODO TESTE] Lista cortada para {max_books} livros.")
                # ---------------------------
                
                print(f"\n[Processamento] Baixando e extraindo EPUBs de {len(paginas)} páginas...")
                
                batch_size = 20
                for i in range(0, len(paginas), batch_size):
                    batch = paginas[i:i + batch_size]
                    tasks = [self.process_book_page(session, url) for url in batch]
                    await asyncio.gather(*tasks)
                    await asyncio.sleep(1) # Respiro para o servidor deles
                    
        finally:
            await self.close_db()
        print("\n[Finalizado] Extração do Projecto Adamastor concluída!")

if __name__ == "__main__":
    pipeline = AdamastorScraperPipeline(max_concurrent=5)
    asyncio.run(pipeline.run(max_books=10))