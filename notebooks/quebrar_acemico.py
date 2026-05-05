import sqlite3
import pandas as pd
import re
import uuid
import hashlib
import random
import shutil
import os
from tqdm import tqdm

# --- CONFIGURAÇÃO DOS BANCOS ---
DB_ORIGEM = "data/dataset_artigo.db"               # O seu banco original (Intocável)
DB_DESTINO = "data/dataset_artigo_balanceado.db"   # O novo banco que será criado
QTD_LONGO = 500
QTD_MUITO_LONGO = 500

def categorizar_tamanho(char_count: int) -> str:
    if 100 <= char_count <= 600:       return "Curto"
    elif 601 <= char_count < 2501:     return "Médio"
    elif 2501 <= char_count < 5000:    return "Médio-Longo"
    elif 5000 <= char_count <= 30000:  return "Longo"
    else:                              return "Muito Longo"

def quebrar_texto_academico(texto: str) -> list[str]:
    """
    Quebra o texto por seções naturais ou, em último caso, por parágrafos.
    """
    padrao_secoes = re.compile(
        r'\n\s*(?:\d+(?:\.\d+)*\s*)?(?:Introdução|Metodologia|Materiais e Métodos|Resultados|Discussão|Conclusão|Considerações Finais|Referências|Bibliografia)\b', 
        re.IGNORECASE
    )
    
    partes_iniciais = padrao_secoes.split(texto)
    chunks_finais = []

    for parte in partes_iniciais:
        parte = parte.strip()
        if len(parte) < 100: 
            continue

        if len(parte) <= 5000:
            chunks_finais.append(parte)
        else:
            paragrafos = [p.strip() for p in parte.split('\n') if p.strip()]
            chunk_atual = ""
            alvo_tamanho = random.choice([400, 1500, 3500]) 

            for p in paragrafos:
                if len(chunk_atual) + len(p) < alvo_tamanho:
                    chunk_atual += p + "\n\n"
                else:
                    if len(chunk_atual) >= 100:
                        chunks_finais.append(chunk_atual.strip())
                    
                    chunk_atual = p + "\n\n"
                    alvo_tamanho = random.choice([400, 1500, 3500])

            if len(chunk_atual) >= 100:
                chunks_finais.append(chunk_atual.strip())

    return chunks_finais

def balancear_banco_seguro():
    if not os.path.exists(DB_ORIGEM):
        print(f"❌ Erro: O banco de origem '{DB_ORIGEM}' não existe.")
        return

    # --- PASSO 1: Leitura do Banco Original ---
    print("📖 Lendo dados do banco original...")
    conn_origem = sqlite3.connect(DB_ORIGEM)
    
    query = f"""
        SELECT * FROM texts WHERE size_category = 'Longo' ORDER BY RANDOM() LIMIT {QTD_LONGO}
    """
    df_longos = pd.read_sql_query(query, conn_origem)

    query = f"""
        SELECT * FROM texts WHERE size_category = 'Muito Longo' ORDER BY RANDOM() LIMIT {QTD_MUITO_LONGO}
    """
    df_muito_longos = pd.read_sql_query(query, conn_origem)
    
    conn_origem.close() # Fechamos a conexão com o original! Ele está seguro.

    df_alvo = pd.concat([df_longos, df_muito_longos], ignore_index=True)
    
    if df_alvo.empty:
        print("⚠️ Nenhum texto gigante encontrado para processar.")
        return

    ids_para_deletar = df_alvo['text_id'].tolist()
    novos_registros = []

    # --- PASSO 2: Processar as Fatias (em memória) ---
    for _, row in tqdm(df_alvo.iterrows(), total=len(df_alvo), desc="✂️ Fracionando Textos"):
        texto_original = row['content']
        pedacos = quebrar_texto_academico(texto_original)

        for pedaco in pedacos:
            char_count = len(pedaco)
            nova_categoria = categorizar_tamanho(char_count)
            
            # Descarta os que ainda ficaram gigantes (falhas de regex)
            if nova_categoria in ["Longo", "Muito Longo"]:
                continue
                
            word_count = len(pedaco.split())
            novo_hash = hashlib.md5(pedaco.encode('utf-8')).hexdigest()

            novos_registros.append((
                str(uuid.uuid4()),          # novo text_id
                pedaco,                     # novo content
                row['label'],               
                row['broad_area'],          
                row['specific_theme'],      
                char_count,                 # novo char_count
                word_count,                 # novo word_count
                nova_categoria,             # nova size_category
                row['creation_date'],       
                row['source_url'],          
                row['source_name'],         
                novo_hash                   # novo content_hash
            ))

    print(f"\n✅ {len(df_alvo)} textos originais renderam {len(novos_registros)} novos fragmentos!")

    # --- PASSO 3: Criar o Novo Banco (Clone + Edição) ---
    print(f"💽 Criando o novo banco de dados: {DB_DESTINO}")
    shutil.copy2(DB_ORIGEM, DB_DESTINO) # Clona o arquivo inteiro
    
    # Conecta no clone para fazer a "cirurgia"
    conn_destino = sqlite3.connect(DB_DESTINO)
    cursor = conn_destino.cursor()

    print("🗑️ Apagando os textos gigantes originais do novo banco...")
    placeholders = ','.join('?' for _ in ids_para_deletar)
    cursor.execute(f"DELETE FROM texts WHERE text_id IN ({placeholders})", ids_para_deletar)

    print("💉 Injetando os novos fragmentos fatiados...")
    cursor.executemany("""
        INSERT OR IGNORE INTO texts 
        (text_id, content, label, broad_area, specific_theme, char_count, word_count, size_category, creation_date, source_url, source_name, content_hash) 
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, novos_registros)

    conn_destino.commit()
    conn_destino.close()
    
    print(f"\n🎉 SUCESSO! O seu dataset balanceado está pronto em: {DB_DESTINO}")

if __name__ == "__main__":
    balancear_banco_seguro()