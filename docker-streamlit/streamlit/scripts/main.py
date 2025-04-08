import streamlit as st

# Configurar o layout e o ícone da página
st.set_page_config(
    page_title="Painel Curupira",
    page_icon="images/curupira.png",  # Caminho para a imagem
    layout="wide"
)

# Define the pages
page_0 = st.Page("pages/0_Boas_Vindas.py", title="Boas Vindas")
page_1 = st.Page("pages/1_Visao_Geral.py", title="Visão Geral")
page_2 = st.Page("pages/2_Graficos.py", title="Gráficos")

# Set up navigation
pg = st.navigation([page_0, page_1, page_2])

# Run the selected page
pg.run()