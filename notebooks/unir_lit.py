import sqlite3
import pandas as pd
import os

# ══════════════════════════════════════════════════════════════════════════════
# 1. CONFIGURAÇÃO DOS CAMINHOS
# ══════════════════════════════════════════════════════════════════════════════

# Coloque aqui o caminho exato dos seus 3 bancos de dados literários
BANCOS_DE_ORIGEM = [
    "data/dataset_literariro_autores.db",
    "data/dataset_livro.db",
    "data/dataset_literariro.db" # Substitua pelo seu terceiro dataset real
]

# Nome do banco de dados final que vai conter a união de todos
BANCO_UNIFICADO_SAIDA = "data/dataset_literario_completo.db"


def unir_bancos():
    print("Iniciando a fusão dos datasets literários...\n")
    dataframes = []

    # ── Passo 1: Ler todos os bancos ─────────────────────────────────────────
    for db_path in BANCOS_DE_ORIGEM:
        if not os.path.exists(db_path):
            print(f"⚠️ [AVISO] Arquivo não encontrado: {db_path}. Verifique o caminho.")
            continue
            
        try:
            conn = sqlite3.connect(db_path)
            # Lê a tabela inteira de textos
            df_temp = pd.read_sql_query("SELECT * FROM texts", conn)
            conn.close()
            
            # Adiciona uma coluna extra só para você saber de onde o texto veio originalmente (opcional mas útil)
            df_temp['banco_origem'] = os.path.basename(db_path)
            
            dataframes.append(df_temp)
            print(f"✅ Lido: {db_path} -> {len(df_temp)} textos encontrados.")
            
        except Exception as e:
            print(f"❌ [ERRO] Falha ao processar {db_path}: {e}")

    # Verifica se conseguiu ler alguma coisa
    if not dataframes:
        print("\n❌ Nenhum banco de dados foi lido com sucesso. Abortando.")
        return

    # ── Passo 2: Fundir (Concatenar) os DataFrames ───────────────────────────
    print("\nFundindo os dados em memória...")
    df_completo = pd.concat(dataframes, ignore_index=True)
    total_inicial = len(df_completo)

    # ── Passo 3: Limpeza de Duplicatas (Garantia de Qualidade) ───────────────
    # É muito comum o mesmo texto ter sido postado em dois sites diferentes.
    # O content_hash garante que não vamos treinar o modelo com dados duplicados.
    print("Verificando textos duplicados (cruzamento entre sites)...")
    df_completo.drop_duplicates(subset=['content_hash'], keep='first', inplace=True)
    
    textos_removidos = total_inicial - len(df_completo)
    if textos_removidos > 0:
        print(f"🧹 Limpeza: {textos_removidos} textos duplicados foram removidos!")

    # Embaralha os textos para que o dataset final não fique ordenado por site
    df_completo = df_completo.sample(frac=1, random_state=42).reset_index(drop=True)

    # ── Passo 4: Salvar no Novo Banco Unificado ──────────────────────────────
    print(f"\nSalvando o dataset consolidado ({len(df_completo)} textos únicos)...")
    
    conn_saida = sqlite3.connect(BANCO_UNIFICADO_SAIDA)
    
    # Salva usando a estrutura do Pandas. O if_exists='replace' garante que se você 
    # rodar o script de novo, ele sobrescreve o arquivo antigo em vez de duplicar tudo.
    df_completo.to_sql('texts', conn_saida, if_exists='replace', index=False)
    conn_saida.close()

    print(f"\n🎉 SUCESSO! Todos os datasets literários foram unidos em: {BANCO_UNIFICADO_SAIDA}")

    # Bônus: Salvar também uma cópia em CSV para facilitar a visualização se você quiser
    caminho_csv = BANCO_UNIFICADO_SAIDA.replace(".db", ".csv")
    df_completo.to_csv(caminho_csv, index=False)
    print(f"📄 Uma cópia de backup em CSV foi salva em: {caminho_csv}")


if __name__ == "__main__":
    unir_bancos()