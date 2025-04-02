import streamlit as st

st.set_page_config(layout="wide")

# Define the pages
page_0 = st.Page("pages/0_Boas_Vindas.py", title="Painel Curupira", icon="👋")
page_1 = st.Page("pages/1_Visao_Geral.py", title="Visão Geral", icon="🔥")
page_2 = st.Page("pages/2_Resumo.py", title="Resumo", icon="📊")
page_3 = st.Page("pages/3_Tendencia.py", title="Tendência", icon="📈")

# Set up navigation
pg = st.navigation([page_0,page_1, page_2, page_3])

# Run the selected page
pg.run()