import duckdb
import os

LAKE_PATH = "data_lake/raw"
DWH_DB = "analytics.duckdb"

def criar_dwh():
    print("🏗️ Construindo o Data Warehouse (Star Schema)...")
    
    if os.path.exists(DWH_DB):
        os.remove(DWH_DB)
        
    con = duckdb.connect(DWH_DB)
    
    print(" - Lendo arquivos Parquet do Data Lake...")
    con.execute(f"CREATE VIEW stg_clientes AS SELECT * FROM '{LAKE_PATH}/clientes.parquet'")
    con.execute(f"CREATE VIEW stg_produtos AS SELECT * FROM '{LAKE_PATH}/produtos.parquet'")
    con.execute(f"CREATE VIEW stg_pedidos AS SELECT * FROM '{LAKE_PATH}/pedidos.parquet'")
    print(" - Criando Dimensões...")
    
    con.execute("""
        CREATE TABLE dim_cliente AS
        SELECT 
            id as id_cliente,
            nome,
            cidade,
            email
        FROM stg_clientes
    """)
    
    con.execute("""
        CREATE TABLE dim_produto AS
        SELECT 
            id as id_produto,
            nome,
            categoria,
            preco as preco_atual
        FROM stg_produtos
    """)
 
    con.execute("""
        CREATE TABLE dim_tempo AS
        SELECT DISTINCT
            data_venda as data_completa,
            strftime(CAST(data_venda AS DATE), '%Y') as ano,
            strftime(CAST(data_venda AS DATE), '%m') as mes,
            strftime(CAST(data_venda AS DATE), '%d') as dia,
            CASE 
                WHEN strftime(CAST(data_venda AS DATE), '%w') IN ('0', '6') THEN 1 
                ELSE 0 
            END as eh_fim_de_semana
        FROM stg_pedidos
    """)
    
    print(" - Criando Tabela Fato...")
    con.execute("""
        CREATE TABLE fact_vendas AS
        SELECT 
            id as id_pedido,
            id_cliente,
            id_produto,
            data_venda,
            quantidade,
            total as valor_total
        FROM stg_pedidos
    """)
    
    print("✅ Data Warehouse modelado com sucesso em 'analytics.duckdb'")
    con.close()

if __name__ == "__main__":
    criar_dwh()