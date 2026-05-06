import sqlite3
import pandas as pd
import uuid
import hashlib
import shutil
import os
from tqdm import tqdm

# --- CONFIGURAÇÃO DOS BANCOS ---
DB_ORIGEM = "data/dataset_literario_completo.db"     # Banco unificado dos 3 datasets literários
DB_DESTINO = "data/dataset_literario_balanceado.db"  # Banco destino para não quebrar o original
QTD_LONGO = 30
ALVO_TAMANHO_MAXIMO = 550  # Focado na categoria "Curto" (< 600 chars)

def categorizar_tamanho(char_count: int) -> str:
    if 100 <= char_count <= 600:       return "Curto"
    elif 601 <= char_count < 2501:     return "Médio"
    elif 2501 <= char_count < 5000:    return "Médio-Longo"
    elif 5000 <= char_count <= 30000:  return "Longo"
    else:                              return "Muito Longo"

def quebrar_texto_literario(texto: str) -> list[str]:
    """
    Fraciona um texto literário em blocos "Curtos" baseando-se estritamente
    nas quebras de linha (parágrafos), preservando a coerência.
    """
    # Quebra por parágrafos duplos ou simples
    paragrafos = [p.strip() for p in texto.split('\n') if p.strip()]
    
    chunks_finais = []
    chunk_atual = ""

    for p in paragrafos:
        # Se adicionar o próximo parágrafo não estourar o alvo de ~550 chars (Curto)
        if len(chunk_atual) + len(p) < ALVO_TAMANHO_MAXIMO:
            chunk_atual += p + "\n\n"
        else:
            # Se o chunk atual já for maior que 100 chars (mínimo exigido), salva.
            if len(chunk_atual) >= 100:
                chunks_finais.append(chunk_atual.strip())
            
            # Começa o próximo chunk com este parágrafo que não coube
            chunk_atual = p + "\n\n"

    # Salva o restolho, se sobrar algo com mais de 100 chars
    if len(chunk_atual) >= 100:
        chunks_finais.append(chunk_atual.strip())

    return chunks_finais

def fracionar_literatura_seguro():
    if not os.path.exists(DB_ORIGEM):
        print(f"❌ Erro: O banco de origem '{DB_ORIGEM}' não existe.")
        return

    # --- PASSO 1: Leitura do Banco Original ---
    print("📖 Lendo dados do banco original...")
    conn_origem = sqlite3.connect(DB_ORIGEM)
    
    # Busca 30 textos que sejam 'Longo'
    query = f"""
        SELECT * FROM texts 
        WHERE size_category IN ('Longo') 
        ORDER BY RANDOM() 
        LIMIT {QTD_LONGO}
    """
    df_alvo = pd.read_sql_query(query, conn_origem)
    conn_origem.close()

    if df_alvo.empty:
        print("⚠️ Nenhum texto longo encontrado para processar.")
        return

    ids_para_deletar = df_alvo['text_id'].tolist()
    novos_registros = []

    # --- PASSO 2: Processar as Fatias (em memória) ---
    print(f"✂️ Processando {len(df_alvo)} textos gigantes...")
    
    for _, row in tqdm(df_alvo.iterrows(), total=len(df_alvo)):
        texto_original = row['content']
        pedacos = quebrar_texto_literario(texto_original)

        for pedaco in pedacos:
            char_count = len(pedaco)
            nova_categoria = categorizar_tamanho(char_count)
            
            # Queremos focar na categoria "Curto", mas se calhar algum cair no "Médio", é tolerável.
            # Ocultamos qualquer falha bizarra que continue gigantesca.
            if nova_categoria in ["Longo"]:
                continue
                
            word_count = len(pedaco.split())
            novo_hash = hashlib.md5(pedaco.encode('utf-8')).hexdigest()

            novos_registros.append((
                str(uuid.uuid4()),          
                pedaco,                     
                row['label'],               
                row['broad_area'],          
                row['specific_theme'],      
                char_count,                 
                word_count,                 
                nova_categoria,             
                row['creation_date'],       
                row['source_url'],          
                row['source_name'],         
                novo_hash                   
            ))

    print(f"\n✅ {len(df_alvo)} textos literários renderam {len(novos_registros)} contos/trechos curtos!")

    # --- PASSO 3: Criar o Novo Banco (Clone + Edição) ---
    print(f"💽 Criando o novo banco de dados clonado: {DB_DESTINO}")
    shutil.copy2(DB_ORIGEM, DB_DESTINO) 
    
    conn_destino = sqlite3.connect(DB_DESTINO)
    cursor = conn_destino.cursor()

    print("🗑️ Deletando os 30 textos monolíticos do novo banco...")
    placeholders = ','.join('?' for _ in ids_para_deletar)
    cursor.execute(f"DELETE FROM texts WHERE text_id IN ({placeholders})", ids_para_deletar)

    print("💉 Injetando as novas 'pílulas' literárias...")
    cursor.executemany("""
        INSERT OR IGNORE INTO texts 
        (text_id, content, label, broad_area, specific_theme, char_count, word_count, size_category, creation_date, source_url, source_name, content_hash) 
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, novos_registros)

    conn_destino.commit()
    conn_destino.close()
    
    print(f"\n🎉 SUCESSO! A literatura balanceada está pronta em: {DB_DESTINO}")

if __name__ == "__main__":
    fracionar_literatura_seguro()