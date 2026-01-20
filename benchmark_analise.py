import sqlite3
import duckdb
import time

OLTP_DB = "sistema_vendas.db"
OLAP_DB = "analytics.duckdb"

def benchmark():
    print("⏱️ INICIANDO BENCHMARK: OLTP (SQLite) vs OLAP (DuckDB)")
    print("="*60)
    
    query_analitica = """
    SELECT 
        p.categoria,
        SUM(v.total) as total_vendas
    FROM pedidos v
    JOIN produtos p ON v.id_produto = p.id
    GROUP BY p.categoria
    ORDER BY total_vendas DESC
    """

    print("\n1. Executando no SQLite (Row-Oriented)...")
    conn_oltp = sqlite3.connect(OLTP_DB)
    start_time = time.time()
    
    cursor = conn_oltp.execute(query_analitica)
    resultado_oltp = cursor.fetchall()
    
    end_time = time.time()
    tempo_oltp = end_time - start_time
    print(f"   Tempo SQLite: {tempo_oltp:.4f} segundos")
    conn_oltp.close()
    
    print("\n2. Executando no DuckDB (Columnar)...")

    query_dw = """
    SELECT 
        p.categoria,
        SUM(f.valor_total) as total_vendas
    FROM fact_vendas f
    JOIN dim_produto p ON f.id_produto = p.id_produto
    GROUP BY p.categoria
    ORDER BY total_vendas DESC
    """
    
    conn_olap = duckdb.connect(OLAP_DB)
    start_time = time.time()
    
    resultado_olap = conn_olap.execute(query_dw).fetchall()
    
    end_time = time.time()
    tempo_olap = end_time - start_time
    print(f"   Tempo DuckDB: {tempo_olap:.4f} segundos")
    conn_olap.close()
    
    print("="*60)
    print(f"CONCLUSÃO: O DuckDB foi {tempo_oltp / tempo_olap:.2f}x mais rápido que o SQLite.")
    print("="*60)

if __name__ == "__main__":
    benchmark()