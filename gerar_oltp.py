import sqlite3
import pandas as pd
from faker import Faker
import random
import os
import time

NUM_CLIENTES = 1000
NUM_PRODUTOS = 50
NUM_PEDIDOS = 100000  
DB_PATH = "sistema_vendas.db"

fake = Faker('pt_BR')

def setup_db():
    if os.path.exists(DB_PATH):
        os.remove(DB_PATH)
    
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    cursor.execute("""
    CREATE TABLE clientes (
        id INTEGER PRIMARY KEY,
        nome TEXT,
        email TEXT,
        cidade TEXT
    )
    """)
    
    cursor.execute("""
    CREATE TABLE produtos (
        id INTEGER PRIMARY KEY,
        nome TEXT,
        categoria TEXT,
        preco REAL
    )
    """)
    
    cursor.execute("""
    CREATE TABLE pedidos (
        id INTEGER PRIMARY KEY,
        id_cliente INTEGER,
        id_produto INTEGER,
        data_venda DATE,
        quantidade INTEGER,
        total REAL,
        FOREIGN KEY(id_cliente) REFERENCES clientes(id),
        FOREIGN KEY(id_produto) REFERENCES produtos(id)
    )
    """)
    conn.commit()
    return conn

def gerar_dados(conn):
    print("🚀 Iniciando simulação do OLTP...")
    
    print(f" - Gerando {NUM_CLIENTES} clientes...")
    clientes = [(i, fake.name(), fake.email(), fake.city()) for i in range(1, NUM_CLIENTES+1)]
    conn.executemany("INSERT INTO clientes VALUES (?,?,?,?)", clientes)
    
    print(f" - Gerando {NUM_PRODUTOS} produtos...")
    categorias = ['Eletrônicos', 'Roupas', 'Casa', 'Livros', 'Esporte']
    produtos = []
    for i in range(1, NUM_PRODUTOS+1):
        nome = f"{fake.word()} {fake.word()}"
        cat = random.choice(categorias)
        preco = round(random.uniform(10, 500), 2)
        produtos.append((i, nome, cat, preco))
    conn.executemany("INSERT INTO produtos VALUES (?,?,?,?)", produtos)
    
    print(f" - Simulando {NUM_PEDIDOS} transações de vendas...")
    pedidos = []
    for i in range(1, NUM_PEDIDOS+1):
        cli_id = random.randint(1, NUM_CLIENTES)
        prod = random.choice(produtos) # (id, nome, cat, preco)
        prod_id = prod[0]
        preco_unit = prod[3]
        qtd = random.randint(1, 5)
        total = round(qtd * preco_unit, 2)
        data = fake.date_between(start_date='-1y', end_date='today')
        
        pedidos.append((i, cli_id, prod_id, data, qtd, total))
        
        if i % 10000 == 0:
            conn.executemany("INSERT INTO pedidos VALUES (?,?,?,?,?,?)", pedidos)
            conn.commit()
            pedidos = []
            print(f"   ... {i} pedidos processados")

    conn.close()
    print(f"✅ Sucesso! Banco OLTP '{DB_PATH}' criado.")

if __name__ == "__main__":
    conn = setup_db()
    gerar_dados(conn)