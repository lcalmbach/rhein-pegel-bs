import streamlit as st
from st_aggrid import AgGrid
import pandas as pd
from datetime import datetime, timedelta, date
import time
import requests
import pytz
import altair as alt
from texts import texts

tz_GMT = pytz.timezone("Europe/London")
seconds_per_day = 0.6
month_names = {1: 'Jan', 2: 'Feb', 3: 'Mrz', 4: 'Apr', 5: 'Mai', 6: 'Jun', 7: 'Jul', 8: 'Aug', 9: 'Sep', 10: 'Okt', 11: 'Nov', 12: 'Dez'}
data_dict = {
    "flow": {
        "url": "https://data.bs.ch/api/records/1.0/search/?dataset=100089&q=timestamp%3E%3D%22{}T00%3A00%3A00Z%22&rows=10000&sort=timestamp&facet=timestamp&fields=abfluss,timestamp",
        "pq_file": "./data/flow.pq",
        "json_fields": ["timestamp", "abfluss"],
        "time_stamp_column": "timestamp",
        "rename_columns": {"timestamp": "zeit"},
        "aggregation_par": "abfluss",
        "aggregation_func": "mean",
    },
    "precipitation": {
        "url": "https://data.bs.ch/api/records/1.0/search/?dataset=100051&q=datum_zeit%3E%3D%22{}T00%3A00%3A00Z%22&rows=1000&sort=datum_zeit&facet=datum_zeit&fields=datum_zeit,prec_mm",
        "pq_file": "./data/prec.pq",
        "json_fields": ["datum_zeit", "prec_mm"],
        "time_stamp_column": "datum_zeit",
        "rename_columns": {"datum_zeit": "zeit"},
        "aggregation_par": "prec_mm",
        "aggregation_func": "sum",
    },
}


class RheinFlow():
    def __init__(self):
        self.flow_df = self.get_data("flow")
        self.precipitation_df = self.get_data("precipitation")
        self.min_date = self.flow_df["date"].min()
        self.max_date = self.flow_df["date"].max()
        # convert back to datetime
        self.min_date = datetime(self.min_date.year, self.min_date.month, self.min_date.day)
        self.max_date = datetime(self.max_date.year, self.max_date.month, self.max_date.day)

    def get_data(self, key: str):
        def data_is_up_to_date(df):
            """
            Verifies whether now() is not more than 2 hours ahead of the most recent record
            in the local data table.
            """
            diff = pd.to_datetime(datetime.today()) - pd.to_datetime(df["date"].max())
            result = diff < timedelta(hours=24)
            return result

        def synch_local_data(df_local):
            def get_url(url_template):
                """
                Builds the url string for the OGD.bs REST-API. unfortunatley the dataset contains data until 2050, so you cannot calculate from the
                most recent records backwards. Therefore the most recent record in the local dataframe is taken, then the number of hour difference
                from now is calculated and the respective number of hours = records calculated. not sure why the api contains the from and to dates
                as well as the number of records, since that makes the system overdetermined, but wit works
                """
                most_recent_record = pd.to_datetime(df["date"].max())
                most_recent_day = most_recent_record.strftime("%Y-%m-%d")
                url = url_template.format(most_recent_day)
                return url

            def extract_data(data):
                """
                converts the json string into a dataframe with only the required columns
                """

                data = data["records"]
                try:
                    df_ogd = pd.DataFrame(data)["fields"]
                except:
                    print(data["records"])
                # unpack records
                df_ogd = pd.DataFrame(x for x in df_ogd)

                df_ogd.rename(columns=data_dict[key]["rename_columns"], inplace=True)
                df_ogd["zeit"] = pd.to_datetime(df_ogd["zeit"])
                df_ogd["date"] = pd.to_datetime(df_ogd["zeit"]).dt.date
                agg_par = data_dict[key]["aggregation_par"]
                df_ogd = (
                    df_ogd.groupby(["date"])[agg_par]
                    .agg(data_dict[key]["aggregation_func"])
                    .reset_index()
                )
                # aggregation renames the parameter column to the aggregation function, e.g. mean, must be named back
                df_ogd.rename(
                    columns={
                        data_dict[key]["aggregation_func"]: data_dict[key][
                            "aggregation_par"
                        ]
                    },
                    inplace=True,
                )
                # make sure there are no duplicate records
                return df_ogd

            def combine_local_remote_data(df_new_data, df_old_data):
                """
                remove last day of local data and add new data, then save as parquet file
                """
                df_old_data = df_old_data[df_old_data["date"] < df_old_data["date"].max()]
                # save data if df_ogd has data, meaning that more recent data was discovered
                df_new_data = df_new_data[df_new_data["date"] > df_old_data["date"].max()]
                if len(df_new_data) > 0:
                    df = pd.concat([df_old_data, df_new_data])
                    try:
                        df.to_parquet(data_dict[key]["pq_file"])
                    except Exception as e:
                        st.warning("Die neusten Daten konnten nicht gespeichert werden")
                        st.write(e)

            url = data_dict[key]["url"]
            data = requests.get(get_url(url)).json()
            df_ogd = extract_data(data)
            df_ogd["date"] = pd.to_datetime(df_ogd["date"])
            df_ogd = combine_local_remote_data(df_ogd, df_local)

            return df

        df = pd.read_parquet(data_dict[key]["pq_file"])
        df["date"] = pd.to_datetime(df["date"])
        if not data_is_up_to_date(df):
            with st.spinner("Daten werden aktualisiert..."):
                df = synch_local_data(df)
        df["date"] = pd.to_datetime(df["date"])
        return df

    def get_bar_chart(self, df, x, y, domain, ytitle):
        chart = (
            alt.Chart(df)
            .mark_bar(width=50)
            .encode(
                x=alt.X(f"{x}:T", axis=alt.Axis(title="")),
                y=alt.Y(
                    f"{y}:Q",
                    axis=alt.Axis(title=ytitle),
                    scale=alt.Scale(domain=domain),
                ),
            )
        )
        return chart

    def get_line_chart(self, df, point_df, x, y, domain, ytitle):
        chart = (
            alt.Chart(df)
            .mark_line(width=50)
            .encode(
                x=alt.X(f"{x}:T", axis=alt.Axis(title="")),
                y=alt.X(
                    f"{y}:Q",
                    axis=alt.Axis(title=ytitle),
                    scale=alt.Scale(domain=domain),
                ),
            )
        )
        chart += (
            alt.Chart(point_df)
            .mark_point(size=60, color='red', filled=True)
            .encode(
                x=alt.X(f"{x}:T", axis=alt.Axis(title="")),
                y=alt.Y(
                    f"{y}:Q",
                    axis=alt.Axis(),
                    scale=alt.Scale(),
                ),
            )
        )
        return chart

    def show_info(self):
        st.write()
        text = texts["info"]
        st.markdown(text)

    def show_animation(self):
        def interrupt_animation():
            st.session_state['stop'] = True

        def render_day(day):
            df = self.flow_df[self.flow_df["date"] >= pd.Timestamp(start_day)]
            df_flow = self.flow_df[self.flow_df["date"] == day]
            wide_chart = self.get_line_chart(df, df_flow, "date", "abfluss", [0, 4000], "Abflussmenge (m³/s")
            wide_plot_placeholder.altair_chart(
                wide_chart.properties(width=600, height=300, title="Abflussmenge Rhein (m³/s)")
            )
            # precipitation plot
            df_prec = self.precipitation_df[self.precipitation_df["date"] == day]
            chart = self.get_bar_chart(df_prec, "date", "prec_mm", [0, 40], "Niederschlag (mm/Tag)")
            anim_prec.altair_chart(
                chart.properties(
                    width=250, height=300, title="Niederschlag, Messtation Binningen (mm)"
                )
            )
            if len(df_prec) > 0:
                value_prec.markdown(f"Niederschlag: {df_prec.iloc[0]['prec_mm'] :.1f} mm")
            else:
                value_prec.markdown(f"Niederschlag: {0 :.1f} mm")

            # flow plot
            
            chart = self.get_bar_chart(df_flow, "date", "abfluss", [0, 4000], "Abflussmenge (m³/s")
            anim_pegel.altair_chart(
                chart.properties(width=250, height=300, title="Abflussmenge Rhein (m³/s)")
            )
            if len(df_flow) > 0:
                value_pegel.markdown(f"Abflussmenge: {df_flow.iloc[0]['abfluss'] :.1f} m³/s")
            else:
                 value_pegel.markdown(f"Abflussmenge: {0 :.1f} m³/s")
        
        day_placeholder = st.empty()
        wide_plot_placeholder = st.empty()  # date selection slider
        col1, col2 = st.columns(2)  # columns with plots and text
        
        with col1:
            anim_prec = st.empty()
            value_prec = st.empty()
            run_button = st.empty()
        with col2:
            anim_pegel = st.empty()
            value_pegel = st.empty()
            stop_button = st.empty()
        
        with col1:
            current_date = datetime.now().date()
            velocity_dic = {
                0.9: "langsam",
                0.6: "mittel",
                0.3: "schnell",
                0.1: "sehr schnell",
            }
            seconds_per_day = st.selectbox(
                "Geschwindigkeit",
                velocity_dic.keys(),
                index=1,
                format_func=lambda x: velocity_dic[x],
            )
            with col2:
                date_30_days_ago = current_date - timedelta(days=90)
                start_day = st.date_input(
                    "Start Datum",
                    min_value=self.min_date,
                    max_value=self.max_date,
                    value=date_30_days_ago,
                )

            render_day(start_day)
            day_placeholder.markdown(f"**{start_day.strftime('%d.%m.%Y')}**")
            with stop_button:
                st.button('Stop Animation', on_click=interrupt_animation())
            with run_button:
                if st.button("Animation starten"):
                    st.session_state['stop'] = False
                    datelist = pd.date_range(start=start_day, end=self.max_date).tolist()
                    # datelist = [date + timedelta(days=x) for x in range((max_date - day).days + 1)]
                    for day in datelist:
                        # day selector is used for display only, the variable day is used from the loop
                        render_day(day)
                        time.sleep(seconds_per_day)
                        day_placeholder.markdown(f"**{day.strftime('%d.%m.%Y')}**")
                        if st.session_state['stop']:
                            break
            
    def show_stats(self):
        col1, col2 = st.columns(2)
        headers = ['Mittel', 'Minimum', 'Maximum']
        df = self.flow_df
        df['year'] = df['date'].dt.year
        df['month'] = df['date'].dt.month
        df_year = df.groupby('year')['abfluss'].agg(['mean', 'min', 'max'])
        df_month = df.groupby('month')['abfluss'].agg(['mean', 'min', 'max'])
        with col1:
            st.markdown('**Jahres-Statistik**')
            df['year'] = df['date'].astype(str)
            df_year = df_year.applymap(lambda x: round(x, 0))
            column_defs = [
                {'headerName': 'year', 'field': 'Jahr', 'width': 50},  # Adjust the width as needed
                {'headerName': 'mean', 'field': 'Mittel', 'width': 50},  # Adjust the width as needed
                {'headerName': 'min', 'field': 'Minimum', 'width': 50},  # Adjust the width as needed
                {'headerName': 'max', 'field': 'Maximum', 'width': 50},  # Adjust the width as needed
            ]
            AgGrid(df_year,
                   col_defs=column_defs
            )
            st.markdown('Jahres Tagesmittel der Abflussmenge [m³/s] sowie Tages-Minimum und -Maximum im Jahr seit 2020. Aktuelles Jahr mit Daten bis zum aktuellen Zeitpunkt.')
        with col2:
            st.markdown('**Monats-Statistik**')
            df['month'] = df['month'].map(month_names)
            df_month.columns = headers
            df_month = df_month.applymap(lambda x: round(x, 0))
            AgGrid(df_month)
            st.markdown('Monatliches Mitttel der Tages-Abflussmenge [m³/s] sowie tägliches Minimum und Maximum pro Monat seit 2020')
        mean = round(df['abfluss'].mean(), 1)
        min = round(df['abfluss'].min(), 1)
        max = round(df['abfluss'].max(), 1)
        day_min = df['date'].loc[df['abfluss'].idxmin()].strftime('%d.%m.%Y')
        day_max = df['date'].loc[df['abfluss'].idxmax()].strftime('%d.%m.%Y')
        flow_year_km3 = round(mean * 3600 * 24 * 365 / 1e9, 1)
        olymp_poolsize = 2.5 * 1e3 # in km3
        pools_no = int(3600 * 24 * 365 / olymp_poolsize)
        amazonas_km3 = 6642
        text = f'''Über den gesamten Zeitraum beträgt das Tagesmittel {mean} m³/s. Der Geringste Abfluss beträgt {min} m³/s und wurde am {day_min} 
gemessen. Der grösste Abfluss beträgt {max} m³/s am {day_max} gemessen. Im Mittel beträgt der jährlich Abfluss somit {flow_year_km3} km³/s. 
Mit diesem Volumen liessen sich {pools_no} olympische Schwimmbecken füllen. Der Abfluss des Rheins ist allerdings immer noch recht bescheiden verglichen mit dem Amazonas, 
dem wasserreichsten Fluss der Welt. Sein Jahresabfluss beträgt {amazonas_km3} km³, also rund {int(round(amazonas_km3 / flow_year_km3, 0))} Mal mehr.'''
        st.markdown(text)

    def show_gui(self):
        st.image(
            "./img/rhein_hochwasser.jpg",
            caption=None,
            width=None,
            use_column_width="auto",
            clamp=False,
            channels="RGB",
            output_format="auto",
        )
        st.markdown("<small>[Quelle Abbildung](https://www.bs.ch/bilddatenbank)<small>", unsafe_allow_html=True)
        st.header("Rhein-Abfluss in Basel")
        tabs = st.tabs(['Info', 'Statistiken', 'Animierte Grafik'])
        
        with tabs[0]:
            self.show_info()
        with tabs[1]:
            self.show_stats()
        with tabs[2]:
            self.show_animation()