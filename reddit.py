import hashlib
import os
import sqlite3
import uuid
from datetime import datetime, timezone
from urllib.parse import urlparse
import zstandard as zstd
import json
import io
import re

from datasets import load_dataset

ZST_FILE_PATH = "data/raw_data/reddit/submissions/RS_2017-03.zst"
# 1. A sua Peneira (Lista de Subreddits em Português)
# Você pode adicionar quantos quiser aqui. Use sempre letras minúsculas.
SUBREDDITS_PT = {
    "futebol": "Esportes",
    "botecodoreddit": "Humor",
    "mejulgue": "Fofocas",
    "gambiarra": "Vida cotidiana",
    "filmeseseries": "Cultura",
    "musicabr": "Cultura",

    "brasil": "Política",
    "conversas": "Vida cotidiana",
    "desabafos": "Desabafos",
    "eusouobabaca": "Histórias pessoais",
    "perguntereddit": "Conselhos",
    "relatosdoreddit": "Histórias pessoais",
    "opiniaoimpopular": "Debates",
    "relacionamentos": "Relacionamentos",
    "sexualidade": "Relacionamentos",

    "antitrampo": "Trabalho",
    "investimentos": "Finanças",
    "farialimabets": "Finanças",
    "golpe": "Avaliações",
    "conselhodecarreira": "Trabalho",
    "empreendedorismo": "Trabalho",

    "brdev": "Tecnologia",
    "programacao": "Tecnologia",
    "computadores": "Tecnologia",
    "hardwarebrasil": "Tecnologia",

    "idiomas": "Educação",
    "livros": "Cultura",
    "filosofia": "Debates",
    "biologiabrasil": "Educação",
    "psicologiabr": "Saúde",
    "direito": "Educação",
    "conselhoslegais": "Conselhos",

    "estudosbr": "Educação",
    "enem": "Educação",
    "concursospublicos": "Educação",
    "faculdadebr": "Educação",
    "usp": "Educação",
    "filosofiabar": "Educação",
    "professoresbr": "Trabalho",

    "saopaulo": "Vida cotidiana",
    "riodejaneiro": "Vida cotidiana",
    "brasilia": "Vida cotidiana",
    "curitiba": "Vida cotidiana",
    "belohorizonte": "Vida cotidiana",
    "recife": "Vida cotidiana",
    "fortaleza": "Vida cotidiana",
    "riograndedosul": "Vida cotidiana",
    "parana": "Vida cotidiana",

    "foradecasa": "Viagens",
    "viagens": "Viagens",
}


DB_PATH = "data/dataset_social.db"
SCHEMA_PATH = "schema.sql"
COMMIT_BATCH_SIZE = 100


def categorizar_tamanho(char_count: int) -> str:
    if 100 <= char_count <= 600:
        return "Curto"
    if 601 <= char_count < 2501:
        return "Médio"
    if 2501 <= char_count < 5000:
        return "Médio-Longo"
    if 5000 <= char_count <= 30000:
        return "Longo"
    if char_count > 30000:
        return "Muito Longo"
    return "Descartar"


def init_db() -> sqlite3.Connection:
    os.makedirs("data", exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
    with open(SCHEMA_PATH, "r", encoding="utf-8") as schema_file:
        conn.executescript(schema_file.read())
    conn.commit()

    cursor = conn.execute("SELECT content_hash FROM texts")
    rows = cursor.fetchall()
    seen_hashes = {row[0] for row in rows if row[0]}
    print(f"[DB] {len(seen_hashes)} textos já existentes carregados.")

    return conn, seen_hashes


def save_record(conn: sqlite3.Connection, record: dict) -> None:
    conn.execute(
        """INSERT OR IGNORE INTO texts
           (text_id, content, label, broad_area, specific_theme,
            char_count, word_count, size_category, creation_date,
            source_url, source_name, content_hash)
           VALUES (?,?,?,?,?,?,?,?,?,?,?,?)""",
        (
            record["text_id"],
            record["content"],
            record["label"],
            record["broad_area"],
            record["specific_theme"],
            record["char_count"],
            record["word_count"],
            record["size_category"],
            record["creation_date"],
            record["source_url"],
            record["source_name"],
            record["content_hash"],
        ),
    )


def extrair_ano(created_utc) -> str:
    if created_utc is None:
        return ""
    try:
        if isinstance(created_utc, (int, float)):
            timestamp = float(created_utc)
            if timestamp > 1e12:
                timestamp /= 1000.0
            return str(datetime.fromtimestamp(timestamp, tz=timezone.utc).year)

        created_str = str(created_utc).strip()
        if not created_str:
            return ""

        if created_str.isdigit():
            timestamp = float(created_str)
            if timestamp > 1e12:
                timestamp /= 1000.0
            return str(datetime.fromtimestamp(timestamp, tz=timezone.utc).year)

        if len(created_str) >= 4 and created_str[:4].isdigit():
            return created_str[:4]
    except (ValueError, OSError, OverflowError):
        return ""

    return ""

print("Conectando ao dataset gigante em modo Streaming...")

# 2. Carrega o dataset SEM baixar (streaming=True)
# Nota: "train" é o nome padrão da divisão em 99% dos datasets do Hugging Face

conn, seen_hashes = init_db()
textos_salvos = 0
quantidade_desejada = 5000  # Quantos textos você quer capturar antes de parar?
textos_analisados = 0

print("Iniciando a garimpagem. Isso pode levar alguns minutos...\n")

# 2. Configura o "Trator" de descompressão
# O max_window_size previne erros com arquivos zst muito grandes
dctx = zstd.ZstdDecompressor(max_window_size=2147483648) 

print("Iniciando a garimpagem em altíssima velocidade...\n")

try:
    # Abre o arquivo zipado em formato binário ('rb')
    with open(ZST_FILE_PATH, 'rb') as fh:
        # Cria um fluxo de descompressão contínuo
        with dctx.stream_reader(fh) as reader:
            # Transforma os bytes descompactados em linhas de texto legíveis (JSON Lines)
            text_stream = io.TextIOWrapper(reader, encoding='utf-8')
            
            for linha_texto in text_stream:
                textos_analisados += 1
                
                # O Pushshift salva um JSON por linha. Vamos carregá-lo.
                try:
                    linha = json.loads(linha_texto)
                except json.JSONDecodeError:
                    continue # Se a linha estiver corrompida, ignora e segue
 
                # Pega o nome do subreddit da linha atual (converte pra minúsculo para garantir)
                subreddit_atual = str(linha.get('subreddit', '')).lower()
                
                # Se o subreddit estiver na nossa lista, nós salvamos!
                if subreddit_atual in SUBREDDITS_PT:
                    
                    # Pega o texto do comentário (no Reddit geralmente chama 'body' ou 'selftext')
                    texto = linha.get("body", "") or linha.get("selftext", "")
                    texto_limpo = str(texto).strip()
                    tema = SUBREDDITS_PT[subreddit_atual]
                    
                    # Ignora comentários apagados ou vazios
                    if texto_limpo and texto_limpo not in ["[deleted]", "[removed]"] and len(texto_limpo) > 100:

                        texto_limpo = re.sub(r'https?://\S+|www\.\S+', '', texto_limpo).strip()

                        char_count = len(texto_limpo)
                        categoria = categorizar_tamanho(char_count)
                        if categoria == "Descartar":
                            continue

                        word_count = len(texto_limpo.split())
                        permalink = linha.get("permalink", "")
                        if permalink:
                            original_url = f"https://www.reddit.com{permalink}"
                        else:
                            original_url = f"https://www.reddit.com/r/{subreddit_atual}"

                        timestamp = extrair_ano(linha.get("created_utc"))

                        ano_str = timestamp[:4]

                        if not (ano_str and ano_str.isdigit()):
                            continue

                        ano = int(ano_str)

                        if ano < 2015 or ano >= 2020:
                            continue

                        texto_hash = hashlib.md5(texto_limpo.encode("utf-8")).hexdigest()

                        if texto_hash in seen_hashes:
                            continue
                        seen_hashes.add(texto_hash)


                        save_record(conn, {
                            "text_id": str(uuid.uuid4()),
                            "content": texto_limpo,
                            "label": 0,
                            "broad_area": "Social",
                            "specific_theme": tema,
                            "char_count": char_count,
                            "word_count": word_count,
                            "size_category": categoria,
                            "creation_date": timestamp[:4],
                            "source_url": original_url,
                            "source_name": subreddit_atual,
                            "content_hash": texto_hash,
                        })
                        textos_salvos += 1

                        if textos_salvos % COMMIT_BATCH_SIZE == 0:
                            conn.commit()

                        # Printa o progresso a cada 100 achados
                        if textos_salvos % 100 == 0:
                            print(f"Salvos: {textos_salvos} | Analisados no total: {textos_analisados}")
                        
                # Para o loop quando atingir a meta
                if textos_salvos >= quantidade_desejada:
                    break
except Exception as e:
    print(f"\n[ERRO CRÍTICO] Ocorreu um problema lendo o arquivo: {e}")
finally:   
    conn.commit()
    conn.close()

    print(f"\nSucesso! {textos_salvos} textos em português salvos no banco '{DB_PATH}'.")
    print(f"O script leu {textos_analisados} linhas do servidor para achar esses textos.")