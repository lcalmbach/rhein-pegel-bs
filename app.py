"""
The Environmental data explorer app allows to explorer environmental datasets. 
"""

import streamlit as st
import altair as alt
import pandas as pd
import numpy as np
from datetime import datetime
import time


__version__ = '0.0.1' 
__author__ = 'Lukas Calmbach'
__author_email__ = 'lcalmbach@gmail.com'
VERSION_DATE = '2021-07-25'
my_name = '🌧️ Rhein-Pegel-BS'
my_kuerzel = "rhein-pegel.bs"
GIT_REPO = 'https://github.com/lcalmbach/rhein-pegel-bs'

APP_INFO = f"""<div style="background-color:powderblue; padding: 10px;border-radius: 15px;">
    <small>App created by <a href="mailto:{__author_email__}">{__author__}</a><br>
    version: {__version__} ({VERSION_DATE})<br>
    <a href="{GIT_REPO}">git-repo</a>
    """

def init():
    st.set_page_config(  # Alternate names: setup_page, page, layout
        layout="centered",  # Can be "centered" or "wide". In the future also "dashboard", etc.
        initial_sidebar_state="auto",  # Can be "auto", "expanded", "collapsed"
        page_title=my_name,  # String or None. Strings get appended with "• Streamlit". 
        page_icon='🌧️',  # String, anything supported by st.image, or None.
    )

@st.cache()
def get_data():
    df_precipitation = pd.read_parquet('./data/prec.pq')
    df_pegel = pd.read_parquet('./data/pegel.pq')
    return df_precipitation, df_pegel

def get_bar_chart(df, x, y, domain, ytitle):
    chart = alt.Chart(df).mark_bar(width = 50).encode(
        x=alt.X(f"{x}:T",
        axis=alt.Axis(title="")), 
        y=alt.X(f"{y}:Q",
        axis=alt.Axis(title=ytitle), 
        scale=alt.Scale(domain=domain),), 
    )
    return chart


def main():
    init()
    st.image('./img/rhein_hochwasser.jpg', caption=None, width=None, use_column_width='auto', clamp=False, channels='RGB', output_format='auto')
    st.markdown('[Quelle Abbildung](https://www.bs.ch/bilddatenbank)')
    st.write()
    st.markdown("**Animation des Rheinabflusses seit Juni 2020**")
    text = """Die Abflussmenge des Rheins hängt wesentlich vom Niederschlag im Einzugsgebiet des Flusses ab. Je länger und intensiver es regnet, desto grösser die Ablussmenge und desto höher der Pegel. Diese App veranschaulicht das Zusammenspiel 
von Niederschlag und Abflussmenge in einer Animation: Zwei Grafiken zeigen für jeden Tag den Niederschlag und die Abflussmenge des Rheins in Basel an. Lege das Startdatum fest und drücke den `Animation starten` Knopf. Anschliessend läuft die Animation bis zum heutigen Tag.  

**Datenquellen (Opendata.bs)**: 
- [Rhein Wasserstand, Pegel und Abfluss](https://data.bs.ch/explore/dataset/100089) 
- [Niederschlag](https://data.bs.ch/explore/dataset/100051) Der Niederschlag der Messtation Binningen wurde stellvertretend für den Niederschlag im Einzugsgebiet verwendet, dies stellt natürlich eine sehr grobe Vereinfachung des Gesamtniederschlags dar.
- [Weitere Infos (BAFU)](https://www.hydrodaten.admin.ch/de/2615.html)
"""
    st.markdown(text)
    df_precipitation, df_pegel = get_data()
    
    min_date = df_pegel['datum'].min()
    max_date = df_pegel['datum'].max()
    start_date = st.date_input("Start-Datum",min_value=min_date, max_value = max_date, value=datetime(2021,1,1))
    if st.button("Animation starten"):
        datelist = pd.date_range(start=start_date, end=max_date).tolist()
        my_bar = st.progress(0)
        day_title = st.empty()
        col1, col2 = st.beta_columns(2)
        
        prec_sum = 0
        with col1:
            anim_prec = st.empty()
            value_prec = st.empty()
            value_prec_sum = st.empty()
        with col2:
            anim_pegel = st.empty()
            value_pegel = st.empty()
        i = 0 
        for day in datelist:
            day_title.markdown(f"**{day :%d.%b %Y}**")
            df = df_precipitation[df_precipitation['datum']==day]
            chart = get_bar_chart(df, 'datum', 'sum', [0, 40], 'Niederschlag (mm/Tag)')
            anim_prec.altair_chart(chart.properties(width=250, height=300, title="Niederschlag, Messtation Binningen (mm)"))
            if len(df)>0:
                value_prec.markdown(f"Niederschlag: {df.iloc[0]['sum'] :.1f} mm")
                prec_sum += df.iloc[0]['sum']
                value_prec_sum.markdown(f"Niederschlag Summe Jahr: {prec_sum :.1f} mm")
            
            df = df_pegel[df_pegel['datum']==day]
            chart = get_bar_chart(df, 'datum', 'abflussmenge', [0,4000], 'Abflussmenge (m³/s')
            anim_pegel.altair_chart(chart.properties(width=250, height=300, title="Abflussmenge Rhein (m³/s)"))
            if len(df)>0:
                value_pegel.markdown(f"Abflussmenge: {df.iloc[0]['abflussmenge'] :.1f} m³/s")
            i += 1
            # reset sum precipitation on january first
            if (day.day == 1) & (day.month == 1):
                prec_sum = 0
            pct = int(i/len(datelist)*100)
            my_bar.progress(pct)
            time.sleep(0.5)


if __name__ == "__main__":
    main()
    



