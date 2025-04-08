import streamlit as st
import base64

# Função para converter imagem em Base64
def get_base64_image(image_path):
    with open(image_path, "rb") as img_file:
        return base64.b64encode(img_file.read()).decode("utf-8")

# Caminho para a imagem
image_path = "images/curupira.png"

# Codificar a imagem em Base64
image_base64 = get_base64_image(image_path)

# Título com imagem no Markdown
st.markdown(
    f"""
    <div style="display: flex; align-items: center;">
        <img src="data:image/png;base64, {image_base64}" alt="Logo" style="width: 50px; height: 50px; margin-right: 10px;">
        <h1 style="color: rgba(230, 57, 70, 0.6);">Painel Curupira</h1>
    </div>
    """,
    unsafe_allow_html=True
)

# Texto introdutório com formatação
st.markdown(
    """
    <div style="background-color: rgba(255, 255, 255, 0.8); padding: 20px; border-radius: 10px; border: 1px solid rgba(230, 57, 70, 0.8);">
        <p style="font-size: 18px; text-align: justify; color: #333;">
        Bem-vinda(o) ao <b>Painel Curupira</b>! Este é um espaço que agrega informações dos focos de queimada do Brasil. 
        As informações utilizadas neste painel vêm do <a href="https://terrabrasilis.dpi.inpe.br/queimadas/portal/dados-abertos/" target="_blank" style="color: rgba(230, 57, 70, 0.8);">Programa Queimadas do INPE</a>.
        </p>
        <p style="font-size: 18px; text-align: justify; color: #333;">
        A ideia é que esse seja um espaço em que os cidadãos possam ter uma visão mais clara da situação atual das queimadas no Brasil.
        </p>
        <p style="font-size: 18px; text-align: justify; color: #333;">
        Além dos dados do INPE, também foram agregadas informações de <a href="https://geoftp.ibge.gov.br/organizacao_do_territorio/malhas_territoriais/malhas_municipais/municipio_2022/Brasil/BR/" target="_blank" style="color: rgba(230, 57, 70, 0.8);">municípios</a> 
        e <a href="https://www.ibge.gov.br/geociencias/informacoes-ambientais/vegetacao/15842-biomas.html?=&t=downloads" target="_blank" style="color: rgba(230, 57, 70, 0.8);">biomas</a> a partir dos dados do IBGE.
        </p>
        <p style="font-size: 18px; text-align: justify; color: #333;">
        Para mais informações sobre como este painel foi criado, acesse o <a href="https://github.com/julianabfreitas/brazil-wildfires" target="_blank" style="color: rgba(230, 57, 70, 0.8);">GitHub</a> do projeto.
        </p>
    </div>
    """,
    unsafe_allow_html=True
)

# Rodapé ou mensagem adicional
st.markdown(
    """
    <footer style="text-align: center; margin-top: 20px; font-size: 14px; color: #888;">
        Desenvolvido por Juliana Bernardes Freitas | <a href="https://github.com/julianabfreitas/brazil-wildfires" target="_blank" style="color: rgba(230, 57, 70, 0.6);">GitHub</a>
    </footer>
    """,
    unsafe_allow_html=True
)

#st.image('images/curupira.png')

#st.markdown('<a href="https://www.flaticon.com/br/icones-gratis/curupira" title="curupira ícones">Curupira ícones criados por Freepik - Flaticon</a>')
