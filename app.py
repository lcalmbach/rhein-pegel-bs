"""
The Environmental data explorer app allows to explorer environmental datasets. 
"""

import streamlit as st
import altair as alt
import pandas as pd
import numpy as np
from datetime import datetime, timedelta, date
import time
import requests
import pytz


__version__ = '0.0.2' 
__author__ = 'Lukas Calmbach'
__author_email__ = 'lcalmbach@gmail.com'
VERSION_DATE = '2021-07-27'
my_name = 'Rhein-Pegel-BS'
my_kuerzel = "rhein-pegel.bs"
GIT_REPO = 'https://github.com/lcalmbach/rhein-pegel-bs'

tz_GMT = pytz.timezone('Europe/London')
APP_INFO = f"""<div style="background-color:powderblue; padding: 10px;border-radius: 15px;">
    <small>App created by <a href="mailto:{__author_email__}">{__author__}</a><br>
    version: {__version__} ({VERSION_DATE})<br>
    <a href="{GIT_REPO}">git-repo</a>
    """
seconds_per_day = 0.6

data_dict = {
    'flow':{
        "url": "https://data.bs.ch/api/records/1.0/search/?dataset=100089&q=timestamp%3E%3D%22{}T00%3A00%3A00Z%22&rows=10000&sort=timestamp&facet=timestamp&fields=abfluss,timestamp",
        "pq_file": "./data/pegel.pq",
        "json_fields": ["timestamp", "abfluss"],
        "time_stamp_column": "timestamp",
        "rename_columns":{"timestamp": "zeit"},
        "aggregation_par": 'abfluss',
        "aggregation_func": 'mean',
    },
    "precipitation": {
        "url": "https://data.bs.ch/api/records/1.0/search/?dataset=100051&q=datum_zeit%3E%3D%22{}T00%3A00%3A00Z%22&rows=1000&sort=datum_zeit&facet=datum_zeit&fields=datum_zeit,prec_mm",
        "pq_file": "./data/prec.pq",
        "json_fields": ["datum_zeit", "prec_mm"],
        "time_stamp_column": "datum_zeit",
        "rename_columns":{"datum_zeit": "zeit"},
        "aggregation_par": 'prec_mm',
        "aggregation_func": 'sum',
    },
}

def init():
    st.set_page_config(  # Alternate names: setup_page, page, layout
        layout="centered",  # Can be "centered" or "wide". In the future also "dashboard", etc.
        initial_sidebar_state="auto",  # Can be "auto", "expanded", "collapsed"
        page_title=my_name,  # String or None. Strings get appended with "• Streamlit". 
        page_icon='🌧️',  # String, anything supported by st.image, or None.
    )


def get_data(key: str):
    def data_is_up_to_date(df):
        """
        Verifies whether now() is not more than 2 hours ahead of the most recent record 
        in the local data table.
        """

        diff = pd.to_datetime(datetime.today()) - pd.to_datetime(df['datum'].max())
        result = (diff < timedelta(hours = 24))
        return result
    

    def synch_local_data(df_local):
        def get_url(url_template):
            """
            Builds the url string for the OGD.bs REST-API. unfortunatley the dataset contains data until 2050, so you cannot calculate from the 
            most recent records backwards. Therefore the most recent record in the local dataframe is taken, then the number of hour difference
            from now is calculated and the respective number of hours = records calculated. not sure why the api contains the from and to dates
            as well as the number of records, since that makes the system overdetermined, but wit works
            """
            most_recent_record = pd.to_datetime(df['datum'].max())
            most_recent_day = most_recent_record.strftime('%Y-%m-%d')
            url = url_template.format(most_recent_day) 
            return url
        
        def extract_data(data):
            """
            converts the json string into a dataframe with only the required columns
            """

            data = data['records']
            try:
                df_ogd = pd.DataFrame(data)['fields']
            except :
                print(data['records'])
            # unpack records
            df_ogd = pd.DataFrame(x for x in df_ogd)

            df_ogd.rename(columns = data_dict[key]['rename_columns'], inplace=True)
            df_ogd['zeit'] = pd.to_datetime(df_ogd['zeit'])
            df_ogd['datum'] = pd.to_datetime(df_ogd['zeit']).dt.date
            agg_par=data_dict[key]['aggregation_par']
            df_ogd = df_ogd.groupby(['datum'])[agg_par].agg(data_dict[key]['aggregation_func']).reset_index()
            # aggregation renames the parameter column to the aggregation function, e.g. mean, must be named back
            df_ogd.rename(columns = {data_dict[key]['aggregation_func']: data_dict[key]['aggregation_par']}, inplace=True)
            st.write(df_ogd)
            # make sure there are no duplicate records
            return df_ogd
        
        def combine_local_remote_data(df_new_data, df_old_data):
            """
            remove last day of local data and add new data, then save as parquet file
            """
            st.write(df_old_data['datum'].max())
            df_old_data = df_old_data[df_old_data['datum'] < df_old_data['datum'].max()]
            st.write(df_old_data['datum'].max())
             # save data if df_ogd has data, meaning that more recent data was discovered

            df_old_data['datum'].min()
            df_new_data = df_new_data[df_new_data['datum'] > df_old_data['datum'].max()]
            df_old_data['datum'].min()
            if len(df_new_data) > 0:
                df = df_old_data.append(df_new_data)
                try:
                    df.to_parquet(data_dict[key]['pq_file'])
                except:
                    st.warning('Die neusten Daten konnten nicht gespeichert werden')

            
        url = data_dict[key]['url']
        data = requests.get(get_url(url)).json()
        df_ogd = extract_data(data)
        df_ogd = combine_local_remote_data(df_ogd, df_local)
        
        return df 
    
    df = pd.read_parquet(data_dict[key]['pq_file'])
    if not data_is_up_to_date(df):
        print("data is out-of-date")
        st.write(df.head())
        df = synch_local_data(df)
        st.write(df.head())
    else:
        print("data is up-to-date")
    return df


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
    def display_text():
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

    def display_day(day):
        # precipitation plot
        df = df_precipitation[df_precipitation['datum']==day]
        # reset sum precipitation on january first
        chart = get_bar_chart(df, 'datum', 'prec_mm', [0, 40], 'Niederschlag (mm/Tag)')
        anim_prec.altair_chart(chart.properties(width=250, height=300, title="Niederschlag, Messtation Binningen (mm)"))
        if len(df)>0:
            value_prec.markdown(f"Niederschlag: {df.iloc[0]['prec_mm'] :.1f} mm")
        
        # flow plot
        df = df_flow[df_flow['datum']==day]
        chart = get_bar_chart(df, 'datum', 'abfluss', [0, 4000], 'Abflussmenge (m³/s')
        anim_pegel.altair_chart(chart.properties(width=250, height=300, title="Abflussmenge Rhein (m³/s)"))
        if len(df)>0:
            value_pegel.markdown(f"Abflussmenge: {df.iloc[0]['abfluss'] :.1f} m³/s")
        
    init()
    display_text()
    
    df_flow = get_data('flow')
    df_precipitation = get_data('precipitation')
    
    min_date = df_flow['datum'].min()
    max_date = df_flow['datum'].max()
    # convert back to datetime
    min_date = datetime(min_date.year, min_date.month, min_date.day)
    max_date = datetime(max_date.year, max_date.month, max_date.day)
    
    run_button = st.empty()
    my_day_selector = st.empty() # date selection slider 
    col1, col2 = st.beta_columns(2) #columns with plots and text
    
    with col1:
        anim_prec = st.empty()
        value_prec = st.empty()
    with col2:
        anim_pegel = st.empty()
        value_pegel = st.empty()
        
    with my_day_selector:
        day = st.slider(f"", min_value=min_date, max_value=max_date)
        day = date(day.year,day.month,day.day)
    display_day(day)

    with run_button:
        if st.button("Animation starten"):
            datelist = pd.date_range(start=day, end=max_date).tolist()
            for day in datelist:
                # day selector is used for display only, the variable day is used from the loop
                with my_day_selector:
                    st.slider(f"{day :%d.%b %Y}", min_value=min_date, max_value=max_date, value = datetime(day.year,day.month,day.day))
                display_day(day)
                
                time.sleep(seconds_per_day)

    st.markdown(APP_INFO, unsafe_allow_html=True)

if __name__ == "__main__":
    main()
    



