import streamlit as st
import duckdb
import pandas as pd
import plotly.express as px
import os

OLAP_DB = "analytics.duckdb"

st.set_page_config(page_title="Data Stack Pro", layout="wide")
st.title("📊 Modern Data Stack Local: Do OLTP ao Analytics")

@st.cache_data
def load_data(query):
    con = duckdb.connect(database=OLAP_DB, read_only=True)
    df = con.execute(query).fetchdf()
    con.close()
    return df

st.sidebar.title("🛠️ Tech Stack")
st.sidebar.markdown(f"""
- **OLTP:** SQLite (Produção)
- **Data Lake:** Parquet Files
- **DWH/OLAP:** DuckDB (Star Schema)
- **Visualização:** Streamlit & Plotly
""")

st.header("Análise Multidimensional (OLAP)")
col1, col2 = st.columns(2)

with col1:
    st.subheader("Vendas por Categoria")
    query_cat = """
    SELECT dp.categoria, SUM(fv.valor_total) as total 
    FROM fact_vendas fv
    JOIN dim_produto dp ON fv.id_produto = dp.id_produto
    GROUP BY 1 ORDER BY 2 DESC
    """
    df_cat = load_data(query_cat)
    fig_cat = px.pie(df_cat, values='total', names='categoria', hole=0.4, color_discrete_sequence=px.colors.qualitative.Pastel)
    st.plotly_chart(fig_cat, width='stretch')

with col2:
    st.subheader("Top Clientes")
    n_clientes = st.slider("Selecione o top N:", 5, 15, 8)
    query_cli = f"""
    SELECT dc.nome, SUM(fv.valor_total) as total 
    FROM fact_vendas fv
    JOIN dim_cliente dc ON fv.id_cliente = dc.id_cliente
    GROUP BY 1 ORDER BY 2 DESC LIMIT {n_clientes}
    """
    df_cli = load_data(query_cli)
    fig_cli = px.bar(df_cli, x='total', y='nome', orientation='h', color='total', color_continuous_scale='Viridis')
    st.plotly_chart(fig_cli, width='stretch')

st.divider()
st.header("🎬 Linha do Tempo Animada")

st.subheader("1. Corrida de Categorias Mensal")
query_anim_bar = """
SELECT 
    strftime(CAST(dt.data_completa AS DATE), '%Y-%m') as mes_ano,
    dp.categoria,
    SUM(fv.valor_total) as total_vendas
FROM fact_vendas fv
JOIN dim_tempo dt ON fv.data_venda = dt.data_completa
JOIN dim_produto dp ON fv.id_produto = dp.id_produto
GROUP BY 1, 2 ORDER BY 1 ASC
"""
df_anim_bar = load_data(query_anim_bar)

fig_bar_move = px.bar(
    df_anim_bar, 
    x="categoria", 
    y="total_vendas", 
    color="categoria",
    animation_frame="mes_ano", 
    range_y=[0, df_anim_bar['total_vendas'].max() * 1.2],
    title="Faturamento por Categoria ao Longo dos Meses"
)

fig_bar_move.layout.updatemenus[0].buttons[0].args[1]['frame']['duration'] = 800
fig_bar_move.layout.updatemenus[0].buttons[0].args[1]['transition']['duration'] = 400
st.plotly_chart(fig_bar_move, width='stretch')

st.subheader("2. Evolução de Performance de Produtos")
st.write("Cada bolha é um produto. O eixo X é o preço, Y é a quantidade e o tamanho é o lucro total.")

query_bubble = """
SELECT 
    strftime(CAST(dt.data_completa AS DATE), '%Y-%m') as mes_ano,
    dp.nome as produto,
    AVG(fv.valor_total / fv.quantidade) as preco_medio,
    SUM(fv.quantidade) as qtd_vendida,
    SUM(fv.valor_total) as receita_total
FROM fact_vendas fv
JOIN dim_tempo dt ON fv.data_venda = dt.data_completa
JOIN dim_produto dp ON fv.id_produto = dp.id_produto
GROUP BY 1, 2 ORDER BY 1 ASC
"""
df_bubble = load_data(query_bubble)

fig_bubble = px.scatter(
    df_bubble,
    x="preco_medio",
    y="qtd_vendida",
    animation_frame="mes_ano",
    animation_group="produto",
    size="receita_total",
    color="produto",
    hover_name="produto",
    size_max=70,
    range_x=[df_bubble['preco_medio'].min() * 0.9, df_bubble['preco_medio'].max() * 1.1],
    range_y=[0, df_bubble['qtd_vendida'].max() * 1.3],
    title="Movimentação de Mercado por Produto"
)

fig_bubble.layout.updatemenus[0].buttons[0].args[1]['frame']['duration'] = 1000
fig_bubble.layout.updatemenus[0].buttons[0].args[1]['transition']['duration'] = 600
st.plotly_chart(fig_bubble, width='stretch')

st.success("🎯 DuckDB processa essas animações instantaneamente devido ao seu motor colunar!")