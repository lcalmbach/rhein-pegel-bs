# from os import supports_follow_symlinks
import streamlit as st
import pandas as pd
import numpy as np
import altair as alt

# from pandas._libs.tslibs.timestamps import Timestamp

import config
from datetime import datetime, timedelta
import tools
import config as cn
from locale import setlocale, LC_ALL

plot_width = 800
plot_height = 300


class App:
    """ """

    def __init__(self, df_data, df_stations, df_parameters):
        setlocale(LC_ALL, "de_CH")  # shows the month names in german
        self.df_parameters = df_parameters
        self.df_data = tools.add_time_columns(df_data)
        self.df_stations = df_stations
        self.dic_stations = df_stations["name"].to_dict()
        self.station = {}
        self.lst_parameters = list(df_parameters.columns)
        self.start_jahr = int(self.df_data["jahr"].min() + 1)
        self.end_jahr = int(self.df_data["jahr"].max())
        self.settings = {}

        self.plot_type_def = {
            "linechart": {
                "legend": "Liniendiagramm",
                "lst_time_agg": {
                    "Jahr": {
                        "col": "mitte_jahr",
                        "agg_interval": "Jahr",
                        "title": "Jahresmittelwerte",
                        "legend": "Jahresmittel",
                        "settings": [
                            "parameters",
                            "date_from",
                            "date_to",
                            "show_band",
                            "show_guidelines",
                        ],
                        "defaults": {
                            "date_from": datetime(self.start_jahr, 1, 1),
                            "date_to": datetime.today(),
                            "show_guidelines": True,
                        },
                        "occurrence_expr": "in",
                        "time_fmt_from_to": "%Y",
                        "time_fmt_occurrence": "%Y",
                        "time_fmt_x_axis": "%Y",
                    },
                    "Monat": {
                        "col": "mitte_monat",
                        "agg_interval": "Monat",
                        "title": "Monats-Mittelwerte",
                        "legend": "Monatsmittel",
                        "settings": [
                            "parameters",
                            "date_from",
                            "date_to",
                            "show_band",
                            "show_guidelines",
                        ],
                        "defaults": {
                            "date_from": datetime.today() - timedelta(365),
                            "date_to": datetime.today(),
                            "show_guidelines": True,
                        },
                        "occurrence_expr": "im",
                        "time_fmt_from_to": "%B %Y",
                        "time_fmt_occurrence": "%B %Y",
                        "time_fmt_x_axis": "%b %y",
                    },
                    "Woche": {
                        "col": "mitte_woche",
                        "agg_interval": "Woche",
                        "title": "Wochen-Mittelwerte",
                        "legend": "Wochenmittel",
                        "settings": [
                            "parameters",
                            "date_from",
                            "date_to",
                            "show_band",
                            "show_guidelines",
                        ],
                        "defaults": {
                            "date_from": datetime(self.start_jahr, 1, 1),
                            "date_to": datetime.today(),
                            "show_guidelines": True,
                        },
                        "occurrence_expr": "in Woche",
                        "time_fmt_from_to": "%B %Y",
                        "time_fmt_occurrence": "%U %Y",
                        "time_fmt_x_axis": "%b %y",
                    },
                    "Tag": {
                        "col": "datum",
                        "agg_interval": "Tag",
                        "title": "Tages-Mittelwerte",
                        "legend": f"Tagesmittel",
                        "settings": [
                            "parameters",
                            "date_from",
                            "date_to",
                            "show_band",
                            "show_guidelines",
                        ],
                        "defaults": {
                            "date_from": datetime.today() - timedelta(days=30),
                            "date_to": datetime.today(),
                            "show_guidelines": True,
                        },
                        "occurrence_expr": "in",
                        "time_fmt_from_to": "%d. %B %Y",
                        "time_fmt_occurrence": "%d. %B %Y",
                        "time_fmt_x_axis": "%d.%m.%y",
                    },
                    "Stunde": {
                        "col": "zeit",
                        "agg_interval": "Stunde",
                        "title": "Stunden-Mittelwerte",
                        "legend": "Stundenmittel",
                        "settings": [
                            "parameters",
                            "date_from",
                            "date_to",
                            "show_guidelines",
                        ],
                        "defaults": {
                            "date_from": datetime.today() - timedelta(days=30),
                            "date_to": datetime.today(),
                            "show_guidelines": True,
                        },
                        "occurrence_expr": "am",
                        "time_fmt_from_to": "%d. %B",
                        "time_fmt_occurrence": "%d. %B %Y %H:%M",
                        "time_fmt_x_axis": "%d.%m.%y",
                    },
                },
            },
            "barchart": {
                "legend": "Säulendiagramm",
                "lst_time_agg": {
                    "Jahr": {
                        "col": "jahr",
                        "agg_interval": "Jahr",
                        "title": "Jahresmittelwerte",
                        "legend": "Jahresmittel",
                        "settings": ["parameters", "years", "show_guidelines"],
                        "defaults": {
                            "parameters": "PM10",
                            "years": [self.start_jahr, self.end_jahr],
                            "show_guidelines": False,
                        },
                        "occurrence_expr": "in",
                    },
                    "Monat": {
                        "col": "monat",
                        "agg_interval": "Monat",
                        "title": "Monats-Mittelwerte",
                        "legend": "Monatsmittel",
                        "settings": ["parameters", "years", "show_guidelines"],
                        "defaults": {
                            "parameters": "PM10",
                            "years": [self.start_jahr, self.end_jahr],
                            "show_guidelines": False,
                        },
                        "occurrence_expr": "im",
                    },
                    "Woche": {
                        "col": "woche",
                        "agg_interval": "Woche",
                        "title": "Wochen-Mittelwerte",
                        "legend": "Wochenmittel",
                        "settings": ["parameters", "years", "show_guidelines"],
                        "defaults": {
                            "parameters": "PM10",
                            "years": [self.start_jahr, self.end_jahr],
                            "show_guidelines": False,
                        },
                        "occurrence_expr": "in Woche",
                    },
                    "Jahr-Monat": {
                        "col": "mitte_monat",
                        "agg_interval": "Jahr und Monat",
                        "title": "Monatsmittelwerte nach Jahr",
                        "legend": "Monatsmittel",
                        "settings": ["parameters", "years", "show_guidelines"],
                        "defaults": {
                            "parameters": "PM10",
                            "years": [self.start_jahr, self.end_jahr],
                            "show_guidelines": False,
                        },
                        "occurrence_expr": "im",
                    },
                    "Jahr-Woche": {
                        "col": "mitte_woche",
                        "agg_interval": "Jahr und Woche",
                        "title": "Wochenmittelwerte nach Jahr",
                        "legend": "Wochenmittel",
                        "settings": ["parameters", "years", "show_guidelines"],
                        "defaults": {
                            "parameters": "PM10",
                            "years": [self.start_jahr, self.end_jahr],
                            "show_guidelines": False,
                        },
                        "occurrence_expr": "in Woche",
                    },
                },
            },
            "boxplot": {
                "legend": "Boxplot",
                "lst_time_agg": {
                    "Jahr": {
                        "col": "mitte_jahr",
                        "agg_interval": "Jahr",
                        "title": "Jahresmittelwerte",
                        "legend": "Jahresmittel",
                        "settings": ["parameters", "years", "show_guidelines"],
                        "defaults": {
                            "parameters": "PM10",
                            "years": [self.start_jahr, self.end_jahr],
                            "show_guidelines": False,
                        },
                        "occurrence_expr": "in",
                        "time_fmt_from_to": "%Y",
                        "time_fmt_occurrence": "%Y",
                        "time_fmt_x_axis": "%Y",
                        "base_values_agg": "datum",
                    },
                    "Monat": {
                        "col": "monat",
                        "agg_interval": "Monat",
                        "title": "Monats-Mittelwerte",
                        "legend": "Monatsmittel",
                        "settings": ["parameters", "years", "show_guidelines"],
                        "defaults": {
                            "parameters": "PM10",
                            "years": [self.start_jahr, self.end_jahr],
                            "show_guidelines": False,
                        },
                        "occurrence_expr": "im",
                        "time_fmt_from_to": "%Y",
                        "time_fmt_occurrence": "%Y",
                        "time_fmt_x_axis": "%Y",
                        "base_values_agg": "datum",
                    },
                    "Woche": {
                        "col": "woche",
                        "agg_interval": "Woche",
                        "title": "Wochen-Mittelwerte",
                        "legend": "Wochenmittel",
                        "settings": ["parameters", "years", "show_guidelines"],
                        "defaults": {
                            "parameters": "PM10",
                            "years": [self.start_jahr, self.end_jahr],
                            "show_guidelines": False,
                        },
                        "occurrence_expr": "in Woche",
                        "time_fmt_from_to": "%Y",
                        "time_fmt_occurrence": "%Y",
                        "time_fmt_x_axis": "%b %Y",
                        "base_values_agg": "datum",
                    },
                    "Jahr-Monat": {
                        "col": "mitte_monat",
                        "agg_interval": "Jahr und Monat",
                        "title": "Monatsmittelwerte nach Jahr",
                        "legend": "Monatsmittel",
                        "settings": ["parameters", "years", "show_guidelines"],
                        "defaults": {
                            "parameters": "PM10",
                            "years": [self.start_jahr, self.end_jahr],
                            "show_guidelines": False,
                        },
                        "occurrence_expr": "im",
                        "time_fmt_from_to": "%Y",
                        "time_fmt_occurrence": "%Y",
                        "time_fmt_x_axis": "%b %Y",
                        "base_values_agg": None,
                    },
                    "Jahr-Woche": {
                        "col": "mitte_woche",
                        "agg_interval": "Jahr und Woche",
                        "title": "Wochenmittelwerte nach Jahr",
                        "legend": "Wochenmittel",
                        "settings": ["parameters", "years", "show_guidelines"],
                        "defaults": {
                            "parameters": "PM10",
                            "years": [self.start_jahr, self.end_jahr],
                            "show_guidelines": False,
                        },
                        "occurrence_expr": "in Woche",
                        "time_fmt_from_to": "%Y",
                        "time_fmt_occurrence": "%Y",
                        "time_fmt_x_axis": "%b %Y",
                        "base_values_agg": None,
                    },
                },
            },
            "heatmap": {
                "legend": "Heatmap",
                "lst_time_agg": {
                    "Monat": {
                        "col": "monat",
                        "agg_interval": "Monat",
                        "y": "jahr",
                        "x_title": "Monat",
                        "y_title": "Jahr",
                        "title": "Monats-Mittelwerte",
                        "legend": "Monatsmittel",
                        "settings": ["parameters", "years", "show_guidelines"],
                        "defaults": {
                            "parameters": "PM10",
                            "years": [self.start_jahr, self.end_jahr],
                            "show_guidelines": False,
                        },
                        "occurrence_expr": "im",
                        "time_fmt_from_to": "%Y",
                        "time_fmt_occurrence": "%B %Y",
                        "time_fmt_x_axis": "%b %Y",
                    },
                    "Woche": {
                        "col": "woche",
                        "agg_interval": "Woche",
                        "y": "jahr",
                        "x_title": "Jahr",
                        "y_title": "Woche",
                        "title": "Wochen-Mittelwerte",
                        "legend": "Wochenmittel",
                        "settings": ["parameters", "years", "show_guidelines"],
                        "defaults": {
                            "parameters": "PM10",
                            "years": [self.start_jahr, self.end_jahr],
                            "show_guidelines": False,
                        },
                        "occurrence_expr": "in Woche",
                        "time_fmt_from_to": "%Y",
                        "time_fmt_occurrence": "%B %Y",
                        "time_fmt_x_axis": "%b %Y",
                    },
                    "Tag": {
                        "col": "tag",
                        "agg_interval": "Jahr und Monat",
                        "y": "monat",
                        "x_title": "Jahr",
                        "y_title": "Monat",
                        "title": "Monatsmittelwerte nach Jahr",
                        "legend": "Monatsmittel",
                        "settings": ["parameters", "years", "show_guidelines"],
                        "defaults": {
                            "parameters": "PM10",
                            "years": [self.start_jahr, self.end_jahr],
                            "show_guidelines": False,
                        },
                        "occurrence_expr": "im",
                        "time_fmt_from_to": "%Y",
                        "time_fmt_occurrence": "%B %Y",
                        "time_fmt_x_axis": "%b %Y",
                    },
                    "Stunde": {
                        "col": "stunde",
                        "agg_interval": "Jahr und Monat",
                        "y": "monat",
                        "x_title": "Stunde",
                        "y_title": "Monat",
                        "title": "Stundenmittelwerte nach Monat",
                        "legend": "Monatsmittel",
                        "settings": ["parameters", "years"],
                        "defaults": {
                            "parameters": "PM10",
                            "years": [self.start_jahr, self.end_jahr],
                            "show_guidelines": False,
                        },
                        "occurrence_expr": "im",
                        "time_fmt_from_to": "%Y",
                        "time_fmt_occurrence": "%B %Y",
                        "time_fmt_x_axis": "%b %Y",
                    },
                },
            },
        }
        plot_legends = [x["legend"] for x in self.plot_type_def.values()]
        plot_keys = [x for x in self.plot_type_def.keys()]
        self.plot_type_options = dict(zip(plot_keys, plot_legends))

    def filter_data(self):
        df = self.df_data

        if "date_from" in self.settings:
            start_series = self.df_data["datum"].min().to_pydatetime().date()
            end_series = self.df_data["datum"].max().to_pydatetime().date()
            if (
                self.settings["date_from"] > start_series
                or self.settings["date_to"] < end_series
            ):
                df = df[
                    (df["zeit"].dt.date >= self.settings["date_from"])
                    & (df["zeit"].dt.date <= self.settings["date_to"])
                ]
        if "years" in self.settings:
            if (
                self.settings["years"][0] != self.start_jahr
                or self.settings["years"][1] != self.end_jahr
            ):
                df = df[
                    df["jahr"].isin(
                        range(self.settings["years"][0], self.settings["years"][1])
                    )
                ]
        if self.settings["agg_time"] == "Monat-Stunde":
            df = df[df["monat"] == self.settings["monat"]]
        return df

    def get_settings(self, required_options, defaults):
        """
        available options: ['agg_time','parameters','date_from','show_band','show_guidelines', 'years']
        """

        self.settings["show_band"] = False
        for x in required_options:
            if x == "parameters":
                self.settings["parameters"] = st.sidebar.multiselect(
                    "Parameter",
                    options=list(self.df_parameters.columns),
                    default=list(self.df_parameters.columns),
                )
            if x == "years":
                self.settings["years"] = st.sidebar.slider(
                    "🔍Jahr", self.start_jahr, self.end_jahr, defaults["years"]
                )
            if x == "date_from":
                self.settings["date_from"] = st.sidebar.date_input(
                    "Von Datum",
                    min_value=datetime(2003, 1, 1),
                    max_value=datetime.now(),
                    value=defaults["date_from"],
                )
                self.settings["date_to"] = st.sidebar.date_input(
                    "Bis Datum",
                    min_value=datetime(2003, 1, 1),
                    max_value=datetime.now(),
                    value=defaults["date_to"],
                )
            if x == "show_band":
                self.settings["show_band"] = st.sidebar.checkbox("Zeige 90% Fläche")
            if x == "show_guidelines":
                self.settings["show_guidelines"] = st.sidebar.checkbox(
                    "Zeige Grenzwerte", True
                )

    def get_lines(self, par):
        lines = []
        colors = []
        if self.settings["show_guidelines"]:
            guidelines = self.df_parameters[par]["guidelines"]
            for gl in guidelines:
                if tools.is_valid_timeagg(
                    gl["time_agg_field"], self.settings["agg_time"]["col"]
                ):
                    overlay = pd.DataFrame(
                        {"y": [gl["value"]], "Legende": [gl["legend"]]}
                    )
                    line = (
                        alt.Chart(overlay)
                        .mark_rule(strokeWidth=2)
                        .encode(y="y", color=alt.Color("Legende:N"))
                    )
                    lines.append(line)
                    colors.append(gl["color"])
        colors.append("steelblue")
        return lines, colors

    def show_boxplot(self):
        def get_text(df, par, par_title):
            t_agg = t_agg = self.settings["agg_time"]
            text = (
                f"Diese Figur zeigt Boxplots von {par} von {self.settings['years'][0]} bis {self.settings['years'][1]}. "
                f" die Werte sind nach {t_agg['agg_interval']} aggregiert. Jede Box zeigt die Verteilung von 50% der Daten (25. - 75. Perzentil). Die Ausgezogenen Linien repräsentieren je "
                "1.5 * die Standardabweichung, was zirka 99% aller Merkmalswerte entspricht. Alle Werte, die ausserhalb dieses Intervalls fallen, werden als Extremwerte bezeichnet und sind "
                "als individuelle Symbole (offene Kreise) geplottet."
            )
            if t_agg["base_values_agg"] != None:
                text += " Um die Übersichtlichkeit bei den Extremwerten zu wahren, wurden die stündlichen Messungen zu Tageswerten aggregiert."
            return text

        def prepare_data(df, par):
            par_title = par.replace(
                ".", ""
            )  # remove dot, as it cannot be displayed in aggrid
            # df['monat'] = df['monat'].replace(config.MONTHS_DICT)
            t_agg = t_agg = self.settings["agg_time"]
            if t_agg["base_values_agg"] != None:
                df = df.groupby([t_agg["col"], t_agg["base_values_agg"]])[par].agg(
                    ["mean"]
                )
                df = df.rename(columns={"mean": par_title}).reset_index()
            else:
                df = df.rename(columns={par: par_title}).reset_index()
            df = df[[par_title, t_agg["col"]]]
            df = df.rename(columns={par: par_title})
            df["Legende"] = par
            if t_agg["col"] == "monat":
                df = df.replace({"monat": cn.MONTHS_DICT})

            return df, par_title

        def prepare_plot(df, par, par_title):
            t_agg = t_agg = self.settings["agg_time"]
            self.settings["lines"], legend_colors = self.get_lines(par)
            self.settings[
                "plot_title"
            ] = f"{par} an Station {self.station['name']}: {t_agg['title']}"
            self.settings["tooltip"] = list(df.columns)
            if t_agg["col"] in [
                "mitte_jahr",
                "mitte_monat",
                "mitte_woche",
                "datum",
                "zeit",
            ]:
                self.settings["x"] = alt.X(
                    f"{t_agg['col']}:T",
                    axis=alt.Axis(
                        title=t_agg["agg_interval"], format=t_agg["time_fmt_x_axis"]
                    ),
                )
            else:
                self.settings["x"] = alt.X(
                    f"{t_agg['col']}:N",
                    axis=alt.Axis(title=t_agg["agg_interval"]),
                    sort=list(cn.MONTHS_REV_DICT.keys()),
                )
            self.settings["y"] = alt.Y(f"{par_title}:Q")
            self.settings["color"] = alt.Color(
                "Legende:N",
                scale=alt.Scale(range=legend_colors),
            )
            return (df,)

        def show_plot(df):
            chart = (
                alt.Chart(df)
                .mark_boxplot()
                .encode(
                    x=self.settings["x"],
                    y=self.settings["y"],
                    color=self.settings["color"],
                    tooltip=self.settings["tooltip"],
                )
            )

            if self.settings["show_guidelines"]:
                for line in self.settings["lines"]:
                    chart += line
            st.altair_chart(
                chart.properties(
                    width=plot_width,
                    height=plot_height,
                    title=self.settings["plot_title"],
                )
            )

        self.get_settings(
            self.settings["agg_time"]["settings"], self.settings["agg_time"]["defaults"]
        )
        for par in self.settings["parameters"]:
            df = self.filter_data()
            df, par_title = prepare_data(df, par)
            if len(df) > 0:
                prepare_plot(df, par, par_title)
                show_plot(df)
                st.markdown(get_text(df, par, par_title))
                with st.beta_expander("Data"):
                    AgGrid(df)
                    st.markdown(
                        tools.get_table_download_link(df), unsafe_allow_html=True
                    )
            else:
                st.warning("Es wurden mit den Filtereinstellungen keine Werte gefunden")

    def show_linechart(self):
        def get_text(df, par):
            def min_max_comment():
                def get_occurrence_expression(t):
                    return (
                        f"{t_agg['occurrence_expr']} {t:{t_agg['time_fmt_occurrence']}}"
                    )

                unit = self.df_parameters[par]["unit"]
                min_val = df["Wert"].min()
                rec = df[df["Wert"] == min_val]
                min_time = rec[t_agg["col"]].iloc[0]

                max_val = df["Wert"].max()
                rec = df[df["Wert"] == max_val]
                max_time = rec[t_agg["col"]].iloc[0]

                avg_val = df["Wert"].mean()

                text = (
                    f" Der durchschnittliche {par}-Wert beträgt {avg_val: .1f}{unit}. Der tiefste Wert von {min_val: .1f}{unit} wurde {get_occurrence_expression(min_time)} gemessen,\n"
                    f" das Maximum von {max_val: .1f}{unit} trat {t_agg['occurrence_expr']} {max_time: {t_agg['time_fmt_occurrence']}} auf."
                )
                return text

            def band_expr():
                if self.settings["show_band"]:
                    return " Die leicht durchsichtige Fläche zeigt die Fläche, welche 90% der Werte beinhaltet (von 5% zu 95% Perzentil)."
                else:
                    return ""

            t_agg = self.settings["agg_time"]
            text = (
                f"Diese Figur zeigt Zeitreihe von {par} von {self.settings['date_from'] :{t_agg['time_fmt_from_to']}} bis {self.settings['date_to'] :{t_agg['time_fmt_from_to']}}. "
                f"Die Messwerte wurden zu einem Wert pro {self.settings['agg_time']['agg_interval']} aggregiert.{min_max_comment()}{band_expr()}"
            )
            return text

        def prepare_data(df, par):
            par_title = par.replace(
                ".", ""
            )  # remove dot, as it cannot be displayed in aggrid
            t_agg = self.settings["agg_time"]
            df = df.melt(id_vars=[t_agg["col"]], value_vars=par)
            if self.settings["show_band"]:
                df = (
                    df.groupby([t_agg["col"], "variable"])["value"]
                    .agg(["mean", tools.percentile(5), tools.percentile(95)])
                    .reset_index()
                )
            else:
                df = (
                    df.groupby([t_agg["col"], "variable"])["value"]
                    .agg(
                        [
                            "mean",
                        ]
                    )
                    .reset_index()
                )

            df = df.rename(columns={"variable": "Legende", "mean": "Wert"})
            return df, par_title

        def prepare_plot(df, par, par_title):
            t_agg = self.settings["agg_time"]
            self.settings["lines"], legend_colors = self.get_lines(par)
            self.settings[
                "plot_title"
            ] = f"{par} an Station {self.station['name']}: {t_agg['title']}"
            self.settings["tooltip"] = list(df.columns)
            self.settings["tooltip"] = [
                alt.Tooltip(f"{t_agg['col']}:T", format=t_agg["time_fmt_occurrence"]),
                alt.Tooltip("Wert:Q", format=".1f"),
            ]
            self.settings["color"] = alt.Color(
                "Legende:N",
                scale=alt.Scale(range=legend_colors),
                # sort = alt.EncodingSortField( 'order', order = 'ascending' ),
            )

            if t_agg["col"] in ["jahr", "monat", "woche"]:
                self.settings["x"] = alt.X(f"{t_agg['col']}:N", axis=alt.Axis(title=""))
            else:
                pass
            self.settings["x"] = alt.X(
                f"{t_agg['col']}:T",
                axis=alt.Axis(title="", format=t_agg["time_fmt_x_axis"]),
            )
            self.settings["y"] = alt.Y(
                f"Wert:Q", axis=alt.Axis(title="Konzentration in µg/m³")
            )
            self.settings["y_par"] = par

        def show_plot(df):
            chart = (
                alt.Chart(df)
                .mark_line()
                .encode(
                    x=self.settings["x"],
                    y=self.settings["y"],
                    color=self.settings["color"],
                    tooltip=self.settings["tooltip"],
                )
                .properties(
                    width=plot_width,
                    height=plot_height,
                    title=self.settings["plot_title"],
                )
            )

            if self.settings["show_band"]:
                band = (
                    alt.Chart(df)
                    .mark_area(opacity=0.3)
                    .encode(
                        x=self.settings["x"],
                        y="percentile_5",
                        y2="percentile_95",
                        color=alt.Color("Legende"),
                    )
                )
                chart += band

            if self.settings["show_guidelines"]:
                for line in self.settings["lines"]:
                    chart += line
            st.altair_chart(chart)

        self.get_settings(
            self.settings["agg_time"]["settings"], self.settings["agg_time"]["defaults"]
        )

        for par in self.settings["parameters"]:
            df = self.filter_data()
            df, par_title = prepare_data(df, par)
            if len(df) > 0:
                prepare_plot(df, par, par_title)
                show_plot(df)
                st.markdown(get_text(df, par))
                with st.beta_expander("Data"):
                    AgGrid(df)
                    st.markdown(
                        tools.get_table_download_link(df), unsafe_allow_html=True
                    )
            else:
                st.warning("Es wurden mit den Filtereinstellungen keine Werte gefunden")

    def show_barchart(self):
        def get_text(df, par, par_title):
            def get_guidelines():
                result = ""
                if self.settings["show_guidelines"]:
                    result = f" Die horizontalen Linien zeigen den Grenzwert für diesen Schadstoff."
                return result

            def min_max_comment():
                def get_occurrence_expression(t):
                    return f"{t_agg['occurrence_expr']} {t}"

                unit = self.df_parameters[par]["unit"]
                min_val = df[par_title].min()
                rec = df[df[par_title] == min_val]
                t_agg = self.settings["agg_time"]
                min_time = rec[t_agg["col"]].iloc[0]

                max_val = df[par_title].max()
                rec = df[df[par_title] == max_val]
                max_time = rec[t_agg["col"]].iloc[0]

                avg_val = df[par_title].mean()

                text = (
                    f" Der durchschnittliche Wert beträgt {avg_val: .1f}{unit}. Der tiefste Wert von {min_val: .1f}{unit} wurde {get_occurrence_expression(min_time)} gemessen,\n"
                    f" das Maximum von {max_val: .1f}{unit} trat {t_agg['occurrence_expr']} {max_time} auf."
                )
                return text

            min_max_comment()
            text = (
                f"Diese Figur zeigt Zeitreihe von {par} von {self.settings['years'][0]} bis {self.settings['years'][1]} an Station {self.station['name']}. "
                f"Die Messwerte wurden zu einem Wert pro {self.settings['agg_time']['agg_interval']} aggregiert (Mittelwert).{min_max_comment()}{get_guidelines()}"
            )
            return text

        def prepare_data(df, par):
            par_title = par.replace(
                ".", ""
            )  # remove dot, as it cannot be displayed in aggrid
            t_agg = self.settings["agg_time"]
            df = df.groupby(t_agg["col"])[par].agg(["mean"])
            df = df.rename(columns={"mean": par_title})
            df["Legende"] = t_agg["legend"]
            df = df.reset_index()
            if t_agg["col"] == "monat":
                df = df.replace({"monat": cn.MONTHS_DICT})

            return df, par_title

        def prepare_plot(df, par, par_title):
            t_agg = self.settings["agg_time"]
            self.settings["lines"], legend_colors = self.get_lines(par)
            self.settings[
                "plot_title"
            ] = f"{par} an Station {self.station['name']}: {t_agg['title']}"
            self.settings["bar_width"] = plot_width / 2 / len(df)
            self.settings["tooltip"] = list(df.columns)
            self.settings["color"] = alt.Color(
                "Legende:N",
                scale=alt.Scale(range=legend_colors),
                # sort = alt.EncodingSortField( 'order', order = 'ascending' ),
            )
            if t_agg["col"] == "monat":
                self.settings["x"] = alt.X(
                    f"{t_agg['col']}:N",
                    axis=alt.Axis(title=""),
                    sort=list(cn.MONTHS_REV_DICT.keys()),
                )
            elif t_agg["col"] in ["jahr", "woche"]:
                self.settings["x"] = alt.X(f"{t_agg['col']}:N", axis=alt.Axis(title=""))
            else:
                self.settings["x"] = alt.X(
                    f"{t_agg['col']}:T", axis=alt.Axis(title="", format="%b %Y")
                )
            if t_agg["col"] == "monat":
                self.settings["y"] = alt.Y(
                    f"{par_title}:Q",
                    axis=alt.Axis(title="Konzentration in µg/m³"),
                )
            else:
                self.settings["y"] = alt.Y(
                    f"{par_title}:Q", axis=alt.Axis(title="Konzentration in µg/m³")
                )
            self.settings["y_par"] = par

            # self.settings['x_scale'] = (1,23)

        def show_plot(df):
            chart = (
                alt.Chart(df)
                .mark_bar(clip=True, width=self.settings["bar_width"])
                .encode(
                    x=self.settings["x"],
                    y=self.settings["y"],
                    color=self.settings["color"],
                    tooltip=self.settings["tooltip"],
                )
            )
            if self.settings["show_guidelines"]:
                for line in self.settings["lines"]:
                    chart += line
            st.altair_chart(
                chart.properties(
                    width=plot_width,
                    height=plot_height,
                    title=self.settings["plot_title"],
                )
            )  # .resolve_scale(color='independent')

        self.get_settings(
            self.settings["agg_time"]["settings"], self.settings["agg_time"]["defaults"]
        )

        for par in self.settings["parameters"]:
            df = self.filter_data()
            df, par_title = prepare_data(df, par)
            if len(df) > 0:
                prepare_plot(df, par, par_title)
                show_plot(df)
                st.markdown(get_text(df, par, par_title))
                with st.beta_expander("Data"):
                    AgGrid(df)
                    st.markdown(
                        tools.get_table_download_link(df), unsafe_allow_html=True
                    )
            else:
                st.warning("Es wurden mit den Filtereinstellungen keine Werte gefunden")

    def show_heatmap(self):
        def get_text(df, par, par_title):
            def min_max_comment():
                def get_occurrence_expression(t):
                    return f"{t_agg['occurrence_expr']} {t}"

                unit = self.df_parameters[par]["unit"]
                min_val = df[par_title].min()
                rec = df[df[par_title] == min_val]
                t_agg = self.settings["agg_time"]
                min_time = rec[t_agg["col"]].iloc[0]

                max_val = df[par_title].max()
                rec = df[df[par_title] == max_val]
                max_time = rec[t_agg["col"]].iloc[0]

                avg_val = df[par_title].mean()

                text = (
                    f" Der durchschnittliche Wert beträgt {avg_val: .1f}{unit}. Der tiefste Wert von {min_val: .1f}{unit} wurde {get_occurrence_expression(min_time)} gemessen,\n"
                    f" das Maximum von {max_val: .1f}{unit} trat {t_agg['occurrence_expr']} {max_time} auf."
                )
                return text

            min_max_comment()
            t_agg = self.settings["agg_time"]
            text = f"Diese Figur zeigt die Heatmap von {par} mit {t_agg['title']}n von {self.settings['years'][0]} bis {self.settings['years'][1]} an Station {self.station['name']}."

            return text

        def prepare_data(df, par):
            par_title = par.replace(
                ".", ""
            )  # remove dot, as it cannot be displayed in aggrid
            df["monat"] = df["monat"].replace(config.MONTHS_DICT)
            t_agg = self.settings["agg_time"]
            df = df.groupby([t_agg["col"], t_agg["y"]])[par].agg(["mean"])
            df["Legende"] = t_agg["legend"]
            df = df.rename(columns={"mean": par_title}).reset_index()
            df = df.reset_index()

            return df, par_title

        def prepare_plot(df, par, par_title):
            t_agg = self.settings["agg_time"]
            self.settings[
                "plot_title"
            ] = f"{par} an Station {self.station['name']}: {t_agg['title']}"
            self.settings["tooltip"] = list(df.columns)

            self.settings["x"] = alt.X(
                f"{t_agg['col']}:N",
                sort=list(cn.MONTHS_REV_DICT.keys()),
                axis=alt.Axis(title=t_agg["x_title"]),
            )
            self.settings["y"] = alt.Y(
                f"{t_agg['y']}:N",
                sort=list(cn.MONTHS_REV_DICT.keys()),
                axis=alt.Axis(title=t_agg["y_title"]),
            )
            self.settings["color"] = alt.Color(f"{par_title}:Q")
            return df

        def show_plot(df):
            chart = (
                alt.Chart(df)
                .mark_rect()
                .encode(
                    x=self.settings["x"],
                    y=self.settings["y"],
                    color=self.settings["color"],
                    tooltip=self.settings["tooltip"],
                )
                .properties(
                    width=plot_width,
                    height=plot_height * 1.5,
                    title=self.settings["plot_title"],
                )
            )
            st.altair_chart(chart)

        self.get_settings(
            self.settings["agg_time"]["settings"], self.settings["agg_time"]["defaults"]
        )
        for par in self.settings["parameters"]:
            df = self.filter_data()
            df, par_title = prepare_data(df, par)
            if len(df) > 0:
                prepare_plot(df, par, par_title)
                show_plot(df)
                st.markdown(get_text(df, par, par_title))
                with st.beta_expander("Data"):
                    AgGrid(df)
                    st.markdown(
                        tools.get_table_download_link(df), unsafe_allow_html=True
                    )
            else:
                st.warning("Es wurden mit den Filtereinstellungen keine Werte gefunden")

    def show_menu(self):
        _plot_type = st.sidebar.selectbox(
            "Grafiktyp",
            options=list(self.plot_type_options.keys()),
            format_func=lambda x: self.plot_type_options[x],
        )
        self.settings["plot_type"] = self.plot_type_def[_plot_type]

        _station_id = st.sidebar.selectbox(
            "Station",
            list(self.dic_stations.keys()),
            format_func=lambda x: self.dic_stations[x],
        )
        self.station = self.df_stations.loc[_station_id]

        time_agg_options = list(self.settings["plot_type"]["lst_time_agg"].keys())
        _time_agg = st.sidebar.selectbox(
            "Aggregiere Messungen nach", options=time_agg_options
        )
        self.settings["agg_time"] = self.settings["plot_type"]["lst_time_agg"][
            _time_agg
        ]

        if _plot_type == "barchart":
            self.show_barchart()
        elif _plot_type == "linechart":
            self.show_linechart()
        elif _plot_type == "boxplot":
            self.show_boxplot()
        elif _plot_type == "heatmap":
            self.show_heatmap()
