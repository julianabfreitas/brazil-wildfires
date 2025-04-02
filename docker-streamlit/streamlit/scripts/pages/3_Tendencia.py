import streamlit as st

st.title("Tendências Temporais e Sazonalidade")

# Conexão com o banco de dados
conn = st.connection("postgresql", type="sql")

# Obter a lista de biomas disponíveis
query = ''' 
SELECT dm.des_sigla AS Sigla, COUNT(*) AS Total
FROM wildfires.dm_municipio dm 
GROUP BY dm.des_sigla;
'''
municipios_df = conn.query(query, ttl="10m")

st.line_chart(municipios_df, x="sigla", y="total")