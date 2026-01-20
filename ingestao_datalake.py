import sqlite3
import pandas as pd
import os

DB_PATH = "sistema_vendas.db"
LAKE_PATH = "data_lake/raw"

def ingestao_full_load():
    print("🌊 Iniciando ingestão para o Data Lake...")
    
    if not os.path.exists(LAKE_PATH):
        os.makedirs(LAKE_PATH)
    
    conn = sqlite3.connect(DB_PATH)
    tabelas = ['clientes', 'produtos', 'pedidos']
    
    for tabela in tabelas:
        print(f" - Extraindo tabela '{tabela}'...")
        df = pd.read_sql_query(f"SELECT * FROM {tabela}", conn)
        caminho_arquivo = f"{LAKE_PATH}/{tabela}.parquet"
        df.to_parquet(caminho_arquivo, index=False)
        print(f"   -> Salvo em {caminho_arquivo}")
        
    conn.close()
    print("✅ Dados brutos carregados no Data Lake.")

if __name__ == "__main__":
    ingestao_full_load()