# script initializes data from a csv file containing all data upto now. the data is aggregated as daily values and saved in the parquet format
# for fast retrievel into pandas. in the app, this local data is read first, if it contains the data from yesterday, it is used as is, otherwise,
# new data is fetched from data.bs and added to the pq, which then is uptodate for the next user.

import pandas as pd

URL_TEMPLATE = "https://data.bs.ch/api/explore/v2.1/catalog/datasets/{}/exports/csv?lang=de&timezone=Europe%2FBerlin&use_labels=false&delimiter=%3B"
imports = ["prec"]
PREC_FILE = "./data/prec.pq"
FLOW_FILE = "./data/flow.pq"

if "prec" in imports:
    print("starting precipitation...")
    url = (
        URL_TEMPLATE.format("100254")
        + "&select=date,rre150d0"
        + "&where=date%3E%3D'2020-01-01'"
    )
    print(url)
    df = pd.read_csv(url, sep=";").sort_values("date")
    df.columns = ["date", "prec_mm"]
    df["date"] = pd.to_datetime(df["date"])
    df.to_parquet(PREC_FILE)
    print("precipitation finished...")

if "flow" in imports:
    print("starting flow...")
    url = URL_TEMPLATE.format("100089") + "&select=timestamp,abfluss,pegel"
    print(url)
    df = pd.read_csv(url, sep=";")
    df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True)
    df["date"] = pd.to_datetime(df["timestamp"]).dt.date
    grouped = df[["date", "abfluss", "pegel"]].groupby("date")
    df_daily = grouped.agg({"abfluss": "mean", "pegel": "mean"}).reset_index()
    df_daily.to_parquet(FLOW_FILE)
    print("flow finished...")

print("flow")
df = pd.read_parquet(FLOW_FILE)
print(df.head())

print("precipitation")
df = pd.read_parquet(PREC_FILE)
print(df.head())
