import streamlit as st
import pandas as pd
import plotly.express as px

st.markdown(
    f"""
    <div style="display: flex; align-items: center;">
        <h1 style="color: rgba(230, 57, 70, 0.6);">Gráficos</h1>
    </div>
    """,
    unsafe_allow_html=True
)

# Texto introdutório com formatação
st.markdown(
    """
    <div style="background-color: rgba(255, 255, 255, 0.8); padding: 20px; border-radius: 10px; border: 1px solid rgba(230, 57, 70, 0.8);">
        <p style="font-size: 18px; text-align: justify; color: #333;">
        Nesta página você pode acompanhar diferentes gráficos que podem contribuir com a análise dos focos de queimadas no Brasil desde 2018. Use os filtros ao lado esquerdo para ter diferentes visões das informações.
        </p>
    </div>
    """,
    unsafe_allow_html=True
)

# Função genérica para consultar os dados com cache
@st.cache_data(ttl=600)  # Cache com duração de 10 minutos
def get_data(query):
    conn = st.connection("postgresql", type="sql")
    return conn.query(query)

# Obter os valores únicos para os filtros, incluindo satélites
@st.cache_data(ttl=600)
def get_filter_options():
    conn = st.connection("postgresql", type="sql")
    biomas_query = "SELECT DISTINCT id_bioma, des_nome FROM wildfires.dm_bioma ORDER BY des_nome;"
    estados_query = "SELECT DISTINCT des_sigla FROM wildfires.dm_municipio ORDER BY des_sigla;"
    municipios_query = "SELECT DISTINCT id_municipio, des_nome FROM wildfires.dm_municipio ORDER BY des_nome;"
    satelites_query = "SELECT DISTINCT des_satelite FROM wildfires.ft_queimada ORDER BY des_satelite;"
    return {
        "biomas": conn.query(biomas_query),
        "estados": conn.query(estados_query),
        "municipios": conn.query(municipios_query),
        "satelites": conn.query(satelites_query),
    }

# Obter os filtros disponíveis
filter_options = get_filter_options()

# Criar os filtros na barra lateral
st.sidebar.header("Filtros")

# Filtro de intervalo de tempo
start_date = st.sidebar.date_input("Data Inicial", value=pd.to_datetime("2025-01-01"))
end_date = st.sidebar.date_input("Data Final", value=pd.to_datetime("2025-01-31"))

# Filtro de satélite
satelites = filter_options["satelites"]
selected_satelite = st.sidebar.selectbox("Selecione o Satélite", options=satelites["des_satelite"].tolist(), index=satelites["des_satelite"].tolist().index("AQUA_M-T"))

# Filtro de bioma
biomas = filter_options["biomas"]
selected_bioma = st.sidebar.selectbox("Selecione o Bioma", options=["Todos"] + biomas["des_nome"].tolist())

# Filtro de estado
estados = filter_options["estados"]
selected_estado = st.sidebar.selectbox("Selecione o Estado", options=["Todos"] + estados["des_sigla"].tolist())

# Filtro de município
municipios = filter_options["municipios"]
selected_municipio = st.sidebar.selectbox("Selecione o Município", options=["Todos"] + municipios["des_nome"].tolist())


# Construir a consulta SQL dinamicamente com base nos filtros
query = f"""
    SELECT fq.id_queimada, fq.qtd_dias_sem_chuva, fq.vl_precipitacao, fq.vl_risco_fogo, fq.vl_frp,
           fq.id_municipio, fq.id_bioma, fq.des_satelite, dm.des_nome AS municipio_nome, dm.des_sigla AS estado_sigla,
           db.des_nome AS bioma_nome, dd.dt_data, ST_AsText(fq.geom_foco) as geom_foco
    FROM wildfires.ft_queimada fq
    INNER JOIN wildfires.dm_data dd ON fq.id_data = dd.id_data
    INNER JOIN wildfires.dm_municipio dm ON fq.id_municipio = dm.id_municipio
    INNER JOIN wildfires.dm_bioma db ON fq.id_bioma = db.id_bioma
    WHERE   fq.des_satelite = '{selected_satelite}' AND 
            dd.dt_data BETWEEN '{start_date}' AND '{end_date}'
"""

# Adicionar filtro de bioma
if selected_bioma != "Todos":
    bioma_id = biomas[biomas["des_nome"] == selected_bioma]["id_bioma"].values[0]
    query += f" AND fq.id_bioma = {bioma_id}"

# Adicionar filtro de estado
if selected_estado != "Todos":
    query += f" AND dm.des_sigla = '{selected_estado}'"

# Adicionar filtro de município
if selected_municipio != "Todos":
    municipio_id = municipios[municipios["des_nome"] == selected_municipio]["id_municipio"].values[0]
    query += f" AND fq.id_municipio = {municipio_id}"

# Executar a consulta
df = get_data(query)

# Verificar se há dados
if df.empty:
    st.warning("Nenhum dado encontrado para os filtros selecionados.")
else:
    # Gráfico de barras: Total de queimadas por bioma
    queimadas_por_bioma = df.groupby("bioma_nome")["id_queimada"].count().reset_index()
    fig_barras = px.bar(
        queimadas_por_bioma,
        x="bioma_nome",
        y="id_queimada",
        title="Total de Queimadas por Bioma",
        labels={"bioma_nome": "Bioma", "id_queimada": "Total de Queimadas"},
        color_discrete_sequence=["#FF5733"]  # Cor personalizada
    )
    st.plotly_chart(fig_barras)

    # Gráfico de linha: Evolução temporal dos focos de queimada
    queimadas_por_data = df.groupby("dt_data")["id_queimada"].count().reset_index()
    fig_linha = px.line(
        queimadas_por_data,
        x="dt_data",
        y="id_queimada",
        title="Evolução Temporal dos Focos de Queimada",
        labels={"dt_data": "Data", "id_queimada": "Total de Focos"},
        color_discrete_sequence=["#33C3FF"]  # Cor personalizada
    )
    st.plotly_chart(fig_linha)

    # Gráfico de pizza: Proporção de queimadas por estado
    queimadas_por_estado = df.groupby("estado_sigla")["id_queimada"].count().reset_index()
    fig_pizza = px.pie(
        queimadas_por_estado,
        names="estado_sigla",
        values="id_queimada",
        title="Proporção de Queimadas por Estado"
    )
    st.plotly_chart(fig_pizza)

    # Gráfico de dispersão: Risco de fogo vs. Precipitação
    fig_dispersao = px.scatter(
        df,
        x="vl_precipitacao",
        y="vl_risco_fogo",
        title="Risco de Fogo vs. Precipitação",
        labels={"vl_precipitacao": "Precipitação (mm)", "vl_risco_fogo": "Risco de Fogo"},
        color_discrete_sequence=["#FFC300"]  # Cor personalizada
    )
    st.plotly_chart(fig_dispersao)