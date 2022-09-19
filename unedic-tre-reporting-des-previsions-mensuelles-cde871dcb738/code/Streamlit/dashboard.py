
import os

import numpy as np
import pandas as pd
import streamlit as st


from pages_streamlit.page_prev import Prev
from pages_streamlit.page_sage import Sage
# from utils.sidebar import sidebar_caption
from utils.dev import * 

# Config the whole app
st.set_page_config(
    page_title="Dashboard trésorerie",
    layout="wide",
    initial_sidebar_state="expanded",
)



def main():
    """A streamlit app template"""

    st.sidebar.title("Choix de page à afficher")

    PAGES = {
        "Données Sage": Sage,
        "Données de prévision": Prev
    }

    # Select pages
    # Use dropdown if you prefer
    selection = st.sidebar.radio("Liste des pages", list(PAGES.keys()))

    page = PAGES[selection]

    ## display the chosen page
    with st.spinner(f"Loading Page {selection} ..."):
        page = page()
        page()


if __name__ == "__main__" :
    main()

#streamlit run code/Streamlit/dashboard.py --server.port 8080 --server.address 127.0.0.1