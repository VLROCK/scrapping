import asyncio
import aiohttp
import feedparser
import time
import os
import uuid
import hashlib
import aiosqlite
from bs4 import BeautifulSoup
from langdetect import detect, DetectorFactory
from langdetect.lang_detect_exception import LangDetectException

DetectorFactory.seed = 0

DB_PATH = "data/dataset_medium_rss.db"

class MediumRSSPipeline:
    def __init__(self, target_tags: list):
        self.target_tags = target_tags
        self.seen_hashes: set = set()
        
    # ── Banco de Dados ───────────────────────────────────────────────────────
    async def init_db(self):
        os.makedirs("data", exist_ok=True)
        self.db = await aiosqlite.connect(DB_PATH)
        with open("schema.sql", "r", encoding="utf-8") as f:
            await self.db.executescript(f.read())
        await self.db.commit()

        async with self.db.execute("SELECT content_hash FROM texts") as cursor:
            self.seen_hashes = {row[0] for row in await cursor.fetchall()}
        print(f"[DB] {len(self.seen_hashes)} textos carregados da memória.")

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

    # ── Processamento do Feed RSS ────────────────────────────────────────────
    def categorizar_tamanho(self, char_count: int) -> str:
        if 100 <= char_count <= 600: return "Curto"
        elif 601 <= char_count < 2501: return "Médio"
        elif 2501 <= char_count < 5000: return "Médio-Longo"
        elif 5000 <= char_count <= 30000: return "Longo"
        elif char_count > 30000: return "Muito Longo"
        return "Descartar"

    async def extrair_feed_tag(self, session: aiohttp.ClientSession, tag: str):
        url = f"https://medium.com/feed/tag/{tag}"
        print(f" -> Lendo o Feed RSS da tag: {tag}...")
        
        try:
            # 1. Faz o download do XML sem disparar o Cloudflare
            async with session.get(url, timeout=15) as response:
                if response.status != 200:
                    print(f"  [Erro] Falha ao ler feed da tag {tag} (HTTP {response.status})")
                    return
                    
                xml_data = await response.text()
                
            # 2. Transforma o XML num "Dicionário/JSON" gigante do Python
            feed = feedparser.parse(xml_data)
            
            # O idioma principal do feed geralmente fica em feed.feed.language
            idioma_feed = feed.feed.get('language', 'Indefinido')
            
            artigos_salvos = 0
            
            for entry in feed.entries:
                # --- A GUILHOTINA DA DATA (< 2018) ---
                if not entry.get('published_parsed'): continue
                ano_pub = entry.published_parsed.tm_year
                
                if ano_pub >= 2018:
                    # Descartamos tudo o que for de 2018 para a frente
                    continue
                
                # O conteúdo real do artigo vem na tag <content:encoded> (HTML)
                html_conteudo = ""
                if 'content' in entry:
                    html_conteudo = entry.content[0].value
                elif 'summary' in entry:
                    html_conteudo = entry.summary
                
                # Usa o BeautifulSoup apenas para limpar as tags HTML do texto
                texto_limpo = BeautifulSoup(html_conteudo, "html.parser").get_text(separator="\n").strip()
                char_count = len(texto_limpo)
                
                if char_count < 200: continue
                
                # --- FILTRO DE IDIOMA (Dupla Segurança) ---
                # Verifica o JSON do feed E faz a deteção direta no texto
                try:
                    idioma_detectado = detect(texto_limpo)
                    if idioma_detectado != 'pt' and not idioma_feed.startswith('pt'):
                        continue
                except LangDetectException:
                    continue

                # --- EXTRAÇÃO DINÂMICA DO TEMA ---
                # O Medium envia todas as tags do autor num array. Pegamos a primeira
                # que não seja a tag principal que pesquisámos, para dar variedade!
                tema_escolhido = tag.title()
                if 'tags' in entry:
                    todas_tags = [t.term for t in entry.tags]
                    temas_alternativos = [t.title() for t in todas_tags if t.lower() != tag.lower()]
                    if temas_alternativos:
                        tema_escolhido = temas_alternativos[0] # Pega a tag secundária

                texto_hash = hashlib.md5(texto_limpo.encode('utf-8')).hexdigest()
                if texto_hash in self.seen_hashes: continue
                self.seen_hashes.add(texto_hash)

                link_artigo = entry.link

                await self.save_record({
                    "text_id":        str(uuid.uuid4()),
                    "content":        texto_limpo,
                    "label":          0,
                    "broad_area":     "Literária",
                    "specific_theme": tema_escolhido,
                    "char_count":     char_count,
                    "word_count":     len(texto_limpo.split()),
                    "size_category":  self.categorizar_tamanho(char_count),
                    "creation_date":  str(ano_pub),
                    "source_url":     link_artigo,
                    "source_name":    "medium.com (RSS)",
                    "content_hash":   texto_hash,
                })
                artigos_salvos += 1
                print(f"✅ [SALVO] Tema: {tema_escolhido} | Ano: {ano_pub} | {char_count} chars")
                
            print(f"  -> Concluído: {tag}. {artigos_salvos} artigos pré-2018 salvos.")

        except Exception as e:
            print(f"  [Erro] Falha catastrófica no feed {tag}: {e}")

    # ── Orquestrador ─────────────────────────────────────────────────────────
    async def run(self):
        await self.init_db()
        print("Iniciando varredura via RSS (Sem Cloudflare)...\n")

        async with aiohttp.ClientSession() as session:
            # Lança tarefas para ler todos os feeds em simultâneo
            tasks = [self.extrair_feed_tag(session, tag) for tag in self.target_tags]
            await asyncio.gather(*tasks)

        if self.db: await self.db.close()
        print("\n[Finalizado] Extração RSS concluída!")


if __name__ == "__main__":
    # Como os feeds têm limite de quantidade, quanto mais tags usarmos, melhor!
    tags_literarias = [
        "conto", "contos", "conto-brasileiro", "crônica", "cronica", 
        "poesia", "poema", "poesias", "literatura", "literatura-brasileira",
        "ficção", "ficcao", "microconto", "texto-autoral", "drama","fantasia","romance","memórias","ensaio","narrativa","história","historia","literario","literária"
    ]

    pipeline = MediumRSSPipeline(target_tags=tags_literarias)
    asyncio.run(pipeline.run())