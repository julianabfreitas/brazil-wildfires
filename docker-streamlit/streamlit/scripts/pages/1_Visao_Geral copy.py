import streamlit as st
import geopandas as gpd
from shapely import wkt
import folium
import random

st.title("Visão Geral")

conn = st.connection("postgresql", type="sql")

# Obter a lista de biomas disponíveis
query_biomas = "SELECT DISTINCT des_nome FROM wildfires.dm_bioma;"
biomas_df = conn.query(query_biomas, ttl="10m")
biomas = biomas_df['des_nome'].tolist()

# Criar duas colunas para os selectboxes
col1, col2 = st.columns(2)

# Adicionar o primeiro selectbox na primeira coluna
with col1:
    # Adicionar o multiselect para os biomas
    biomas_selecionados = st.multiselect("Biomas:", biomas)

# Adicionar a condição para filtrar os biomas selecionados
if biomas_selecionados:
    condicao_bioma = f'''WHERE des_nome IN ({', '.join([f"'{bioma}'" for bioma in biomas_selecionados])})'''
else:
    condicao_bioma = ""  # Sem filtro, retorna todos os biomas

# Consultar os dados do bioma selecionado
query = f"SELECT id_bioma, vl_codigo, des_nome, st_astext(geom_bioma) as geom_bioma FROM wildfires.dm_bioma {condicao_bioma};"
df = conn.query(query, ttl="10m")

# Converter a coluna de geometria para um GeoDataFrame
df['geom_bioma'] = df['geom_bioma'].apply(wkt.loads)  # Convert WKT to Shapely geometry
gdf = gpd.GeoDataFrame(df, geometry='geom_bioma')

# Garantir que o GeoDataFrame tenha um CRS (Sistema de Referência de Coordenadas)
gdf.set_crs(epsg=4326, inplace=True)  # Assumindo que os dados estão em WGS84 (latitude/longitude)

# Criar um dicionário de cores aleatórias para cada bioma
unique_biomas = gdf['des_nome'].unique()
color_map = {bioma: f"#{random.randint(0, 0xFFFFFF):06x}" for bioma in unique_biomas}

# Criar um mapa Folium centralizado na localização média das geometrias
if not gdf.empty:
    center_lat = gdf.geometry.centroid.y.mean()
    center_lon = gdf.geometry.centroid.x.mean()
    m = folium.Map(location=[center_lat, center_lon], zoom_start=4)

    # Adicionar os polígonos ao mapa usando GeoJson com cores personalizadas
    def style_function(feature):
        bioma_name = feature['properties']['des_nome']
        return {
            'fillColor': color_map[bioma_name],
            'color': 'black',  # Cor da borda
            'weight': 1,       # Espessura da borda
            'fillOpacity': 0.6 # Opacidade do preenchimento
        }

    folium.GeoJson(
        gdf,
        name="Polígonos",
        style_function=style_function,
        tooltip=folium.GeoJsonTooltip(fields=['vl_codigo', 'des_nome'], aliases=['Código:', 'Bioma:'])
    ).add_to(m)

    # Renderizar o mapa Folium no Streamlit
    st.components.v1.html(m._repr_html_(), width=800, height=600)
else:
    st.warning("Nenhum dado encontrado para o bioma selecionado.")