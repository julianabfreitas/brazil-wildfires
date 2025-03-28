import streamlit as st

# Define the pages
page_1 = st.Page("pages/1_Visao_Geral.py", title="Visão Geral", icon="🔥")
page_2 = st.Page("pages/page_2.py", title="Page 2", icon="❄️")
page_3 = st.Page("pages/page_3.py", title="Page 3", icon="🎉")

# Set up navigation
pg = st.navigation([page_1, page_2, page_3])

# Run the selected page
pg.run()