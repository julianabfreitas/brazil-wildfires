import streamlit as st
import geopandas as gpd
from shapely import wkt, from_wkb
import pandas as pd
import pydeck as pdk

# Título com imagem no Markdown
st.markdown(
    f"""
    <div style="display: flex; align-items: center;">
        <h1 style="color: rgba(230, 57, 70, 0.6);">Visão Geral</h1>
    </div>
    """,
    unsafe_allow_html=True
)

# Texto introdutório com formatação
st.markdown(
    """
    <div style="background-color: rgba(255, 255, 255, 0.8); padding: 20px; border-radius: 10px; border: 1px solid rgba(230, 57, 70, 0.8);">
        <p style="font-size: 18px; text-align: justify; color: #333;">
        Nesta página você pode acompanhar a <b>situação geral dos focos de queimada no Brasil</b>. Os dados possuem informações de 2018 até os dias de hoje. Use os filtros ao lado esquerdo para ter diferentes visões das informações.
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

# Cálculos para os cards
total_focos = len(df)
total_municipios = df['id_municipio'].nunique()
total_biomas = df['id_bioma'].nunique()
media_dias_sem_chuva = df['qtd_dias_sem_chuva'].mean()
media_precipitacao = df['vl_precipitacao'].mean()


# Exibir os cards acima do mapa
st.markdown("""
<p></p>
<div style="display: flex; flex-wrap: wrap; justify-content: space-around; margin-bottom: 20px;">
    <div style="text-align: center; margin-bottom: 10px; display: flex; flex-direction: column; align-items: center;">
        <h7 style="margin-bottom: 5px; color: rgba(230, 57, 70, 0.8);">Focos Totais</h7>
        <div style="background-color: rgba(255, 255, 255, 0.8); border: 3px solid rgba(230, 57, 70, 0.8); border-radius: 15px; padding: 10px; width: 120px; height: 60px; display: flex; align-items: center; justify-content: center;">
            <p style="font-size: 20px; color: rgba(230, 57, 70, 0.8); margin: 0;">{}</p>
        </div>
    </div>
    <div style="text-align: center; margin-bottom: 10px; display: flex; flex-direction: column; align-items: center;">
        <h7 style="margin-bottom: 5px; color: rgba(230, 57, 70, 0.8);">Municípios Afetados</h7>
        <div style="background-color: rgba(255, 255, 255, 0.8); border: 3px solid rgba(230, 57, 70, 0.8); border-radius: 15px; padding: 10px; width: 120px; height: 60px; display: flex; align-items: center; justify-content: center;">
            <p style="font-size: 20px; color: rgba(230, 57, 70, 0.8); margin: 0;">{}</p>
        </div>
    </div>
    <div style="text-align: center; margin-bottom: 10px; display: flex; flex-direction: column; align-items: center;">
        <h7 style="margin-bottom: 5px; color: rgba(230, 57, 70, 0.8);">Biomas Afetados</h7>
        <div style="background-color: rgba(255, 255, 255, 0.8); border: 3px solid rgba(230, 57, 70, 0.8); border-radius: 15px; padding: 10px; width: 120px; height: 60px; display: flex; align-items: center; justify-content: center;">
            <p style="font-size: 20px; color: rgba(230, 57, 70, 0.8); margin: 0;">{}</p>
        </div>
    </div>
    <div style="text-align: center; margin-bottom: 10px; display: flex; flex-direction: column; align-items: center;">
        <h7 style="margin-bottom: 5px; color: rgba(230, 57, 70, 0.8);">Média Dias Sem Chuva</h7>
        <div style="background-color: rgba(255, 255, 255, 0.8); border: 3px solid rgba(230, 57, 70, 0.8); border-radius: 15px; padding: 10px; width: 120px; height: 60px; display: flex; align-items: center; justify-content: center;">
            <p style="font-size: 20px; color: rgba(230, 57, 70, 0.8); margin: 0;">{:.2f}</p>
        </div>
    </div>
    <div style="text-align: center; margin-bottom: 10px; display: flex; flex-direction: column; align-items: center;">
        <h7 style="margin-bottom: 5px; color: rgba(230, 57, 70, 0.8);">Média Precipitação</h7>
        <div style="background-color: rgba(255, 255, 255, 0.8); border: 3px solid rgba(230, 57, 70, 0.8); border-radius: 15px; padding: 10px; width: 120px; height: 60px; display: flex; align-items: center; justify-content: center;">
            <p style="font-size: 20px; color: rgba(230, 57, 70, 0.8); margin: 0;">{:.2f}</p>
        </div>
    </div>
</div>
""".format(total_focos, total_municipios, total_biomas, media_dias_sem_chuva, media_precipitacao), unsafe_allow_html=True)

# Converter a coluna de geometria para um GeoDataFrame
df['geom_foco'] = df['geom_foco'].apply(wkt.loads)  # Convert WKT to Shapely geometry
#df['geom_foco'] = df['geom_foco'].apply(from_wkb) 
gdf = gpd.GeoDataFrame(df, geometry='geom_foco')

# Garantir que o GeoDataFrame tenha um CRS (Sistema de Referência de Coordenadas)
gdf.set_crs(epsg=4326, inplace=True)  # Assumindo que os dados estão em WGS84 (latitude/longitude)

# Converter o GeoDataFrame para um DataFrame do Pandas com colunas de latitude e longitude
gdf['latitude'] = gdf.geometry.y
gdf['longitude'] = gdf.geometry.x
data = gdf[['id_queimada', 'latitude', 'longitude', 'municipio_nome']]  # Selecionar apenas as colunas necessárias

# Criar o mapa com Pydeck
if not data.empty:
    # Configurar a camada de pontos
    scatterplot_layer = pdk.Layer(
        "ScatterplotLayer",
        data=data,
        get_position="[longitude, latitude]",  # Coordenadas dos pontos
        get_radius=500,  # Raio dos pontos (em metros)
        get_fill_color=[255, 0, 0, 140],  # Cor dos pontos (RGBA)
        pickable=True,  # Permitir interação (exibir informações ao clicar)
    )

    # Configurar o viewport inicial do mapa
    view_state = pdk.ViewState(
        latitude=-15.6479,
        longitude=-56.0599,
        zoom=3,
        pitch=0,
    )

    # Criar o mapa Pydeck
    r = pdk.Deck(
        layers=[scatterplot_layer],
        initial_view_state=view_state,
        map_style='mapbox://styles/mapbox/streets-v12',
        tooltip={"text": "ID: {id_queimada}"},  # Tooltip ao passar o mouse
    )

    # Renderizar o mapa no Streamlit
    st.pydeck_chart(r)
else:
    st.warning("Nenhum dado encontrado para os filtros selecionados.")