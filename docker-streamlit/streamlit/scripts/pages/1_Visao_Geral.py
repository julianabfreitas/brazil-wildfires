import streamlit as st
import pandas as pd
import geopandas as gpd
from shapely import wkt
import folium

st.markdown("# Visão Geral")

# Initialize connection.
conn = st.connection("postgresql", type="sql")

# Obter a lista de biomas disponíveis
query_biomas = "SELECT DISTINCT des_nome FROM wildfires.dm_bioma;"
biomas_df = conn.query(query_biomas, ttl="10m")
biomas = biomas_df['des_nome'].tolist()

# Adicionar a opção "Todos" no início da lista de biomas
biomas.insert(0, "Todos")

# Criar um seletor para o usuário escolher o bioma
bioma_selecionado = st.selectbox("Selecione o bioma:", biomas)

# Adicionar a condição para filtrar o bioma selecionado
if bioma_selecionado == "Todos":
    condicao_bioma = ""  # Sem filtro, retorna todos os biomas
else:
    condicao_bioma = f"WHERE des_nome = '{bioma_selecionado}'"

# Consultar os dados do bioma selecionado
query = f"SELECT id_bioma, vl_codigo, des_nome, st_astext(geom_bioma) as geom_bioma FROM wildfires.dm_bioma {condicao_bioma};"
df = conn.query(query, ttl="10m")

# Converter a coluna de geometria para um GeoDataFrame
df['geom_bioma'] = df['geom_bioma'].apply(wkt.loads)  # Convert WKT to Shapely geometry
gdf = gpd.GeoDataFrame(df, geometry='geom_bioma')

# Garantir que o GeoDataFrame tenha um CRS (Sistema de Referência de Coordenadas)
gdf.set_crs(epsg=4326, inplace=True)  # Assumindo que os dados estão em WGS84 (latitude/longitude)

# Criar um mapa Folium centralizado na localização média das geometrias
if not gdf.empty:
    center_lat = gdf.geometry.centroid.y.mean()
    center_lon = gdf.geometry.centroid.x.mean()
    m = folium.Map(location=[center_lat, center_lon], zoom_start=4)

    # Adicionar os polígonos ao mapa usando GeoJson
    folium.GeoJson(gdf, name="Polígonos").add_to(m)

    # Renderizar o mapa Folium no Streamlit
    st.components.v1.html(m._repr_html_(), width=800, height=600)
else:
    st.warning("Nenhum dado encontrado para o bioma selecionado.")