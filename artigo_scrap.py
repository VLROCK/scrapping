import asyncio
import aiohttp
from bs4 import BeautifulSoup
import uuid
import fasttext
import hashlib
import trafilatura
import aiosqlite
import re
import os
import json

BASE_DIR    = os.path.dirname(os.path.abspath(__file__))
DB_PATH     = os.path.join(BASE_DIR, "data", "dataset_artigo.db")
SCHEMA_PATH = os.path.join(BASE_DIR, "schema.sql")
LANG_MODEL_PATH = os.path.join(BASE_DIR, "models", "lid.176.ftz")

# Endpoint de metadados leve — usado para pré-filtro de idioma e área
API_DISCOVERY_URL = "http://articlemeta.scielo.org/api/v1/article/identifiers/?collection=scl&limit={limit}&offset={offset}&from=2017-01-01"
API_META_URL      = "http://articlemeta.scielo.org/api/v1/article/?collection=scl&code={pid}&format=json"
API_HTML_URL      = "https://www.scielo.br/article/{pid}/?lang=pt"

AREA_MAP = {
    "Agricultural Sciences":         "Estudos ambientais",
    "Applied Social Sciences":       "Sociologia",
    "Biological Sciences":           "Biologia",
    "Engineering":                   "Engenharia",
    "Exact and Earth Sciences":      "Matemática",
    "Health Sciences":               "Medicina",
    "Human Sciences":                "História",
    "Linguistics, Letters and Arts": "Linguística",
    "Others":                        "Artigo Científico",
}

# ── Banco ────────────────────────────────────────────────────────────────────

async def init_db():
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    db = await aiosqlite.connect(DB_PATH)
    with open(SCHEMA_PATH, "r", encoding="utf-8") as f:
        await db.executescript(f.read())
    await db.commit()
    return db

async def save_record(db, record: dict):
    try:
        await db.execute(
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
        await db.commit()
    except Exception as e:
        print(f"[Erro BD] {e}")

def categorizar_tamanho(char_count: int) -> str:
    if 100 <= char_count <= 600:      return "Curto"
    elif 601 <= char_count < 2501:    return "Médio"
    elif 2501 <= char_count < 5000:   return "Médio-Longo"
    elif 5000 <= char_count <= 30000: return "Longo"
    elif char_count > 30000:          return "Muito Longo"
    return "Descartar"

# ── Discovery com paginação ──────────────────────────────────────────────────

async def discovery_scielo(session, total: int = 500) -> list[str]:
    todos_pids = []
    limit, offset = 100, 0

    while len(todos_pids) < total:
        url = API_DISCOVERY_URL.format(limit=limit, offset=offset)
        try:
            async with session.get(url, timeout=aiohttp.ClientTimeout(total=30)) as r:
                if r.status != 200:
                    break
                # ← corrigido: API retorna text/plain, não application/json
                text = await r.text()
                dados = json.loads(text)
                pids = [item['code'] for item in dados.get('objects', [])]
                if not pids:
                    break
                todos_pids.extend(pids)
                offset += limit
                print(f"  [Discovery] {len(todos_pids)} PIDs coletados...")
        except Exception as e:
            print(f"  [Erro Discovery] {e}")
            break

    return todos_pids[:total]

# ── Pré-filtro via metadados JSON (evita baixar HTML inútil) ─────────────────

async def get_article_meta(session, pid: str) -> dict | None:
    """
    Busca metadados leves do artigo.
    Retorna dict com 'languages' e 'subject_areas', ou None se falhar.
    """
    url = API_META_URL.format(pid=pid)
    try:
        async with session.get(url, timeout=aiohttp.ClientTimeout(total=15)) as r:
            if r.status != 200:
                return None
            text = await r.text()
            data = json.loads(text)

            # Extrair idiomas disponíveis
            # O campo 'fulltexts' lista as línguas com texto completo
            fulltexts = data.get('fulltexts', {}).get('html', {})
            languages = list(fulltexts.keys()) if fulltexts else []

            url_pt = fulltexts.get('pt')

            if not languages:
                v40 = data.get('article', {}).get('v40', [])
                languages = [lang.get('_') for lang in v40 if lang.get('_')]

            # 2. Áreas do Conhecimento (Tags v441 e v440 ficam no bloco 'title')
            bloco_title = data.get('title', {})
            
            # v441 = Grande Área (ex: "Applied Social Sciences")
            v441 = bloco_title.get('v441', [])
            area_grande = v441[0].get('_', 'Others') if v441 else 'Others'
            
            # v440 = Disciplina Específica (ex: "COMUNICAÇÃO")
            v440 = bloco_title.get('v440', [])
            area_especifica = v440[0].get('_', '').title() if v440 else ''

            # 3. Tipo de Documento e Ano
            doc_type = data.get('document_type', 'article')
            ano = data.get('publication_year', 'Desconhecida')

            return {
                "languages":   languages,
                "url_pt":      url_pt,
                "area":        area_grande,
                "sub_area":    area_especifica,
                "doc_type":    doc_type,
                "ano":         ano,
            }
    except Exception as e:
        print(f"  [Meta] Falha no artigo {pid}: {e}")
        return None

# ── Extração principal ───────────────────────────────────────────────────────

async def extrair_artigo(
    session, semaphore, hashes_lock,
    pid: str, seen_hashes: set, seen_pids: set, db,
    lang_model
) -> bool:

    # Controle de PIDs visitados — evita re-baixar entre execuções
    if pid in seen_pids:
        return False

    async with semaphore:

        # ── Pré-filtro de idioma via metadados (sem baixar HTML) ─────────
        meta = await get_article_meta(session, pid)
        if not meta:
            return False

        if 'pt' not in [l.lower() for l in meta['languages']]:
            print(f"[{pid}] Ignorado: sem versão PT (idiomas: {meta['languages']})")
            return False

        specific_theme = meta['area']
        ano_pub        = meta['ano']
        

        # ── Download do HTML em português ────────────────────────────────
        url_html = meta.get('url_pt')
        if not url_html:
            url_html = f"https://www.scielo.br/article/{pid}/?lang=pt"

        headers  = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"}

        try:
            async with session.get(url_html, timeout=aiohttp.ClientTimeout(total=25),
                                   headers=headers) as r:
                if r.status != 200:
                    return False
                html_data = await r.read()
        except Exception as e:
            print(f"[{pid}] Falha de conexão: {e}")
            return False

    # ── Limpeza HTML e extração ──────────────────────────────────────────
    soup = BeautifulSoup(html_data, 'html.parser')
    for tag in soup.find_all(
        re.compile(r'^section$|^div$'),
        {'class': re.compile(r'ref-list|documentReference|references', re.I)}
    ):
        tag.decompose()
    for tag in soup.find_all(id=re.compile(r'article-back|references', re.I)):
        tag.decompose()

    texto = trafilatura.extract(
        str(soup), url=url_html, target_language="pt",
        include_comments=False, include_tables=False
    )

    if not texto:
        return False
    
    # ── 1. FILTRO ANTI-PDF ──────────────────────────────────────────────
    texto_lower = texto.lower()
    if "texto disponível apenas em pdf" in texto_lower or "texto completo dispon" in texto_lower:
        print(f"[{pid}] Descartado: Página fantasma (Apenas PDF disponível).")
        return False
    
    # ── GUILHOTINA DO ABSTRACT ───────────────────────────────────────────
    # Se a palavra "abstract" aparecer solta no texto, descartamos na hora.
    if re.search(r'\babstract\b', texto, re.IGNORECASE):
       print(f"[{pid}] Descartado: Vazamento da seção ABSTRACT no texto.")
       return False

    # Corte de referências
    texto = re.split(
        r'\n(?:Referências|Referências Bibliográficas|Bibliografia|References|Referencias|Agradecimentos)\s*(?:\n|$)',
        texto, flags=re.IGNORECASE
    )[0].strip()

    # ── Validação com fasttext (segunda linha de defesa) ─────────────────
    if lang_model:
        amostra = texto.replace('\n', ' ')
        if len(amostra) > 4000:
            meio    = len(amostra) // 2
            amostra = amostra[meio-1500:meio+1500]
        label, _ = lang_model.predict(amostra, k=1)
        if label[0] != '__label__pt':
            print(f"[{pid}] Miolo em idioma estrangeiro ({label[0]}), descartado.")
            return False

    char_count = len(texto)
    categoria  = categorizar_tamanho(char_count)
    if categoria == "Descartar":
        return False

    texto_hash = hashlib.md5(texto.encode('utf-8')).hexdigest()

    # ── Lock para evitar race condition no seen_hashes ───────────────────
    async with hashes_lock:
        if texto_hash in seen_hashes:
            return False
        seen_hashes.add(texto_hash)

    await save_record(db, {
        "text_id":        str(uuid.uuid4()),
        "content":        texto,
        "label":          0,
        "broad_area":     "Acadêmica",
        "specific_theme": specific_theme,   # ← agora usa o AREA_MAP
        "char_count":     char_count,
        "word_count":     len(texto.split()),
        "size_category":  categoria,
        "creation_date":  ano_pub,          # ← vem dos metadados
        "source_url":     url_html,
        "source_name":    "scielo.br",
        "content_hash":   texto_hash,
    })

    print(f"[{pid}] SALVO | {specific_theme} | {ano_pub} | {char_count} chars")
    return True

async def discovery_scielo_batch(session, limit: int = 100, offset: int = 0) -> list[str]:
    """Busca um lote específico de PIDs para alimentar o robô continuamente."""
    url = API_DISCOVERY_URL.format(limit=limit, offset=offset)
    try:
        async with session.get(url, timeout=aiohttp.ClientTimeout(total=30)) as r:
            if r.status == 200:
                text = await r.text()
                dados = json.loads(text)
                return [item['code'] for item in dados.get('objects', [])]
    except Exception as e:
        print(f"  [Erro Discovery Lote] {e}")
    return []

# ── Orquestrador ─────────────────────────────────────────────────────────────

async def run_scielo_pipeline(meta_alvo: int = 200, max_concurrent: int = 10):

    # Carrega fasttext uma vez, passa como parâmetro (sem global)
    lang_model = None
    if os.path.exists(LANG_MODEL_PATH):
        try:
            lang_model = fasttext.load_model(LANG_MODEL_PATH)
            print(f"[OK] fastText carregado: {LANG_MODEL_PATH}")
        except Exception as e:
            print(f"[Aviso] fastText falhou ({e}). Usando só pré-filtro de metadados.")
    else:
        print("[Aviso] Modelo fastText não encontrado. Usando só pré-filtro de metadados.")

    db = await init_db()

    async with db.execute("SELECT content_hash FROM texts") as cur:
        seen_hashes = {row[0] for row in await cur.fetchall()}

    # PIDs já visitados (evita re-download entre execuções)
    # Extrai o PID da source_url salva
    async with db.execute("SELECT source_url FROM texts WHERE source_name='scielo.br'") as cur:
        seen_pids = set()
        for (url,) in await cur.fetchall():
            m = re.search(r'/article/([^/]+)/', url or '')
            if m:
                seen_pids.add(m.group(1))

    print(f"[DB] {len(seen_hashes)} textos | {len(seen_pids)} PIDs já visitados.")

    semaphore   = asyncio.Semaphore(max_concurrent)
    hashes_lock = asyncio.Lock()

    salvos_sessao = 0
    offset = len(seen_pids) # Começa a buscar a partir do que já vimos para não repetir do zero
    limit = 100

    async with aiohttp.ClientSession() as session:
        while salvos_sessao < meta_alvo:
            print(f"\n[Discovery] Buscando lote de {limit} PIDs (Offset: {offset})...")
            pids_lote = await discovery_scielo_batch(session, limit=limit, offset=offset)
            
            if not pids_lote:
                print("[!] Fim da linha. Não há mais artigos disponíveis na SciELO para essa data.")
                break
                
            offset += limit
            
            # Filtra os que o robô já viu em dias anteriores
            pids_novos = [p for p in pids_lote if p not in seen_pids]
            if not pids_novos:
                continue
                
            print(f" -> Processando {len(pids_novos)} artigos inéditos. Filtrando inglês...")
            
            tasks = [
                extrair_artigo(session, semaphore, hashes_lock, pid, seen_hashes, seen_pids, db, lang_model)
                for pid in pids_novos
            ]
            
            # Executa o lote em paralelo
            resultados = await asyncio.gather(*tasks)
            
            # Conta quantos conseguiram passar pela guilhotina do idioma e salvar
            sucessos_lote = sum(r for r in resultados if r)
            salvos_sessao += sucessos_lote
            
            # Marca todos do lote como visitados para nunca mais bater neles
            seen_pids.update(pids_novos)
            
            print(f"==================================================")
            print(f"[STATUS] Meta: {salvos_sessao} de {meta_alvo} artigos salvos.")
            print(f"==================================================")

    await db.close()
    print(f"\n[Finalizado] A meta foi atingida! {salvos_sessao} artigos inseridos na base.")

    await db.close()
    print(f"\n[Finalizado] {salvos_sessao}/{len(pids_novos)} artigos salvos.")


if __name__ == "__main__":
    asyncio.run(run_scielo_pipeline(meta_alvo=5000, max_concurrent=10))