import sqlite3
import pandas as pd
import os

# --- 1. Configurações e Caminhos dos Bancos de Dados ---
# Ajuste os nomes de acordo com o que tem na sua pasta 'data/'
DB_LIXO = "data/dataset_lixo.db"
DB_LITERARIO = "data/dataset_literariro_autores.db"  # Conforme discutimos, o de autores
DB_INFORMACIONAL = "data/dataset_informacional.db"
DB_SOCIAL = "data/dataset_social.db"
DB_ACADEMICO = "data/dataset_artigo.db"
DB_JORNALISTICO = "data/dataset_jornalistico.db" # Assumindo que o do Wayback Machine chama-se assim

DB_SAIDA = "data/dataset_gabarito_llm.db"

# Quantidades desejadas (Total: 400 amostras -> 200 lixo / 200 bons)
AMOSTRAS_LIXO = 200
AMOSTRAS_BONS_POR_CLASSE = 40

def coletar_amostra(caminho_db: str, quantidade: int, label_correta: int, origem: str) -> pd.DataFrame:
    """Conecta ao banco, pega uma amostra aleatória e retorna um DataFrame formatado."""
    if not os.path.exists(caminho_db):
        print(f"⚠️ [AVISO] Banco não encontrado: {caminho_db}. Ignorando...")
        return pd.DataFrame()

    try:
        conn = sqlite3.connect(caminho_db)
        
        # Puxa apenas o ID e o Texto. ORDER BY RANDOM() garante a aleatoriedade da amostra
        query = f"""
            SELECT text_id, content 
            FROM texts 
            ORDER BY RANDOM() 
            LIMIT {quantidade}
        """
        df = pd.read_sql_query(query, conn)
        conn.close()

        # Adiciona as colunas obrigatórias para o teste do LLM
        df['label_correta'] = label_correta
        df['label_ia'] = None          # Será preenchido pela API do LLM depois
        df['origem_db'] = origem       # Útil para você rastrear se a IA errar muito num tema específico

        print(f"✅ {len(df)} amostras coletadas de {origem}.")
        return df

    except Exception as e:
        print(f"❌ [ERRO] Falha ao ler {caminho_db}: {e}")
        return pd.DataFrame()

def criar_dataset_gabarito():
    print("Iniciando a criação do Dataset de Gabarito (LLM-as-a-Judge)...\n")
    dataframes = []

    # --- 2. Coletar a Classe Negativa (Lixo) -> Label 1 ---
    df_lixo = coletar_amostra(DB_LIXO, AMOSTRAS_LIXO, label_correta=1, origem="Lixo/Ruído")
    dataframes.append(df_lixo)

    # --- 3. Coletar as Classes Positivas (Textos Bons) -> Label 0 ---
    fontes_boas = [
        (DB_LITERARIO, "Literário"),
        (DB_INFORMACIONAL, "Informacional"),
        (DB_SOCIAL, "Social"),
        (DB_ACADEMICO, "Acadêmico"),
        (DB_JORNALISTICO, "Jornalístico")
    ]

    for caminho, nome_origem in fontes_boas:
        df_bom = coletar_amostra(caminho, AMOSTRAS_BONS_POR_CLASSE, label_correta=0, origem=nome_origem)
        dataframes.append(df_bom)

    # --- 4. Unir, Embaralhar e Salvar ---
    # Concatena todos os pedaços que conseguimos puxar
    df_completo = pd.concat(dataframes, ignore_index=True)

    # Embaralha todas as linhas (frac=1) para que o LLM não teste 200 lixos seguidos e depois 200 textos bons
    df_completo = df_completo.sample(frac=1, random_state=42).reset_index(drop=True)

    # Renomeia colunas para o padrão exato que você pediu
    df_completo.rename(columns={'text_id': 'id', 'content': 'texto'}, inplace=True)
    
    # Organiza a ordem das colunas para ficar visualmente bonito
    df_completo = df_completo[['id', 'texto', 'label_correta', 'label_ia', 'origem_db']]

    print(f"\n📊 Dataset Final montado com {len(df_completo)} textos.")
    print(f" -> {len(df_completo[df_completo['label_correta'] == 1])} classificados como Lixo (1)")
    print(f" -> {len(df_completo[df_completo['label_correta'] == 0])} classificados como Texto Bom (0)")

    # Salva no novo banco SQLite
    conn_saida = sqlite3.connect(DB_SAIDA)
    df_completo.to_sql("gabarito_llm", conn_saida, if_exists="replace", index=False)
    conn_saida.close()

    print(f"\n💾 Salvo com sucesso no banco: {DB_SAIDA} (Tabela: gabarito_llm)")

if __name__ == "__main__":
    criar_dataset_gabarito()