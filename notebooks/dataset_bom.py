import sqlite3
import pandas as pd
import os

# --- 1. Caminhos dos Bancos de Dados ---
# Ajuste os nomes para corresponder exatamente à sua pasta data/
DB_SOCIAL = "data/dataset_social.db"
DB_LITERARIO = "data/dataset_literario_balanceado.db" 
DB_INFORMACIONAL = "data/dataset_informacional.db"  
DB_ACADEMICO = "data/dataset_artigo_balanceado.db"  
DB_JORNALISTICO = "data/dataset_jornal_balanceado2.db"    

# Fontes exclusivamente de textos BONS
FONTES_DADOS = [
    (DB_SOCIAL, "Social"),
    (DB_LITERARIO, "Literário"),
    (DB_ACADEMICO, "Acadêmico"),
    (DB_JORNALISTICO, "Jornalístico"),
    (DB_INFORMACIONAL, "Informacional")
]

# Nome do arquivo focado apenas nos bons
DB_SAIDA = "data/dataset_bons.db"
AMOSTRAS_POR_CLASSE = 500  # 5 classes * 500 = 2500 textos totais

def coletar_amostra_estratificada(caminho_db: str, quantidade_total: int, origem: str) -> pd.DataFrame:
    """
    Coleta uma amostra de um banco de dados, variando os tamanhos de forma igualitária,
    e injeta a nova coluna 'label' = 0.
    """
    if not os.path.exists(caminho_db):
        print(f"⚠️ [AVISO] Arquivo não encontrado: {caminho_db}. Pulando...")
        return pd.DataFrame()

    try:
        conn = sqlite3.connect(caminho_db)
        
        # Lê os campos originais mapeando os nomes para o novo formato
        query = """
            SELECT 
                text_id AS id, 
                content AS texto, 
                broad_area, 
                char_count, 
                size_category 
            FROM texts
        """
        df_completo = pd.read_sql_query(query, conn)
        conn.close()

        if df_completo.empty:
            return pd.DataFrame()

        # Cria a nova coluna 'label' forçando o valor 0 (Texto Bom)
        df_completo['label'] = 0
        
        # --- Amostragem Estratificada por Tamanho ---
        categorias_tamanho = df_completo['size_category'].unique()
        qtd_por_tamanho = quantidade_total // len(categorias_tamanho) if len(categorias_tamanho) > 0 else quantidade_total
        
        amostras = []
        restante = quantidade_total

        # Tenta pegar a cota igualitária de textos para cada tamanho
        for cat in categorias_tamanho:
            df_cat = df_completo[df_completo['size_category'] == cat]
            
            # Se tivermos menos textos do que o necessário, pegamos os que têm
            qtd_pegar = min(len(df_cat), qtd_por_tamanho)
            if qtd_pegar > 0:
                amostras.append(df_cat.sample(n=qtd_pegar, random_state=42))
                restante -= qtd_pegar

        df_final = pd.concat(amostras, ignore_index=True)

        # Se faltaram textos para bater a cota (ex: categoria muito pequena),
        # preenchemos com os textos que sobraram independentemente do tamanho.
        if restante > 0:
            textos_ja_pegos = df_final['id'].tolist()
            df_sobra = df_completo[~df_completo['id'].isin(textos_ja_pegos)]
            
            qtd_extra = min(len(df_sobra), restante)
            if qtd_extra > 0:
                df_extra = df_sobra.sample(n=qtd_extra, random_state=42)
                df_final = pd.concat([df_final, df_extra], ignore_index=True)

        print(f"✅ Coletados {len(df_final)} textos da fonte: {origem} (Variando tamanhos)")
        
        # Retorna o DataFrame com as colunas na ordem desejada
        colunas_finais = ['id', 'texto', 'broad_area', 'char_count', 'size_category', 'label']
        return df_final[colunas_finais]

    except Exception as e:
        print(f"❌ [ERRO] Falha ao processar {caminho_db}: {e}")
        return pd.DataFrame()

def montar_dataset_final():
    print("Iniciando a montagem do Dataset de Textos Bons (Label 0)...\n")
    dataframes = []

    for caminho_db, nome_origem in FONTES_DADOS:
        df_amostra = coletar_amostra_estratificada(
            caminho_db=caminho_db, 
            quantidade_total=AMOSTRAS_POR_CLASSE, 
            origem=nome_origem
        )
        if not df_amostra.empty:
            dataframes.append(df_amostra)

    if not dataframes:
        print("\n❌ Nenhum dado foi coletado. Abortando.")
        return

    # Une todas as amostras
    df_dataset_final = pd.concat(dataframes, ignore_index=True)

    # Embaralha brutalmente (frac=1) para garantir distribuição aleatória no treino
    df_dataset_final = df_dataset_final.sample(frac=1, random_state=42).reset_index(drop=True)

    print(f"\n📊 Resumo do Dataset Final:")
    print(f" -> Total de Textos: {len(df_dataset_final)}")
    print(f" -> Distribuição de Labels: \n{df_dataset_final['label'].value_counts().to_string()}")
    print(f" -> Distribuição de Tamanhos: \n{df_dataset_final['size_category'].value_counts().to_string()}")

    # Salva no formato SQLite
    conn_saida = sqlite3.connect(DB_SAIDA)
    df_dataset_final.to_sql("dataset", conn_saida, if_exists="replace", index=False)
    conn_saida.close()

    # Salva uma cópia em CSV para facilitar inspeção e treino
    caminho_csv = DB_SAIDA.replace(".db", ".csv")
    df_dataset_final.to_csv(caminho_csv, index=False, encoding='utf-8')

    print(f"\n💾 Salvo com sucesso!")
    print(f" -> SQLite: {DB_SAIDA}")
    print(f" -> CSV: {caminho_csv}")

if __name__ == "__main__":
    montar_dataset_final()