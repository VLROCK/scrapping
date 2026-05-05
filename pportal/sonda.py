import urllib.request
import json

# Pegue qualquer PID que o seu robô baixou recentemente e cole aqui
PID = "S2358-04292016000600001" # Substitua por um PID real do seu log

# A URL da API que retorna os metadados brutos
URL = f"http://articlemeta.scielo.org/api/v1/article/?collection=scl&code={PID}&format=json"

print(f"Baixando metadados do artigo {PID}...")

try:
    with urllib.request.urlopen(URL) as response:
        # Lê a resposta da API e transforma em um dicionário Python
        dados_brutos = json.loads(response.read().decode('utf-8'))
        
        # Salva o dicionário em um arquivo JSON com indentação (bonito para leitura humana)
        nome_arquivo = f"debug_{PID}.json"
        with open(nome_arquivo, "w", encoding="utf-8") as f:
            json.dump(dados_brutos, f, indent=4, ensure_ascii=False)
            
    print(f"✅ Sucesso! Abra o arquivo '{nome_arquivo}' no seu editor de código.")
    print("Dica: Dê um Ctrl+F e procure por 'Sciences', 'Health', 'Human' ou 'subject' para achar a chave exata.")
    
except Exception as e:
    print(f"❌ Erro ao acessar a API: {e}")