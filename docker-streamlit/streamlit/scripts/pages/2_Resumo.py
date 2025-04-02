import streamlit as st

st.title("Resumo das Condições Climáticas")

# Conexão com o banco de dados
conn = st.connection("postgresql", type="sql")

# Obter a lista de biomas disponíveis
query_estados = 'SELECT DISTINCT dm.des_sigla FROM wildfires.dm_municipio DM ORDER BY dm.des_sigla;'

estados_df = conn.query(query_estados, ttl="10m")
estados = estados_df['des_sigla'].tolist()

estados_selecionados = st.selectbox("Estados:", estados)

# Adicionar a condição para filtrar os biomas selecionados
if estados_selecionados:
    condicao_estado = f'''WHERE des_sigla = '{estados_selecionados}' '''
else:
    condicao_estado = f'''WHERE des_sigla = '{estados[0]}' '''  # Sem filtro, retorna todos os biomas

# Consultar os dados do bioma selecionado
query = f''' 
    SELECT COUNT(*) AS total
    FROM wildfires.dm_municipio dm 
    {condicao_estado}
    GROUP BY dm.des_sigla;
'''

df = conn.query(query, ttl="10m")

if df.empty:
    st.warning("Nenhum dado encontrado para o estado selecionado.")
else:
    st.markdown(f"A quantidade de municípios é: {df['total'][0]}")

