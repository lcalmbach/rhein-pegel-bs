# script initializes data from a csv file containing all data upto now. the data is aggregated as daily values and saved in the parquet format
# for fast retrievel into pandas. in the app, this local data is read first, if it contains the data from yesterday, it is used as is, otherwise, 
# new data is fetched from data.bs and added to the pq, which then is uptodate for the next user.

import pandas as pd

df = pd.read_csv("./data/100051.csv", sep=';')

df['Datum/Zeit']=pd.to_datetime(df['Datum/Zeit'])
df['datum'] = pd.to_datetime(df['Datum/Zeit']).dt.date
df = df.groupby(['datum'])['PREC [mm]'].agg(['sum']).reset_index()
df.rename(columns = {'sum': 'prec_mm'}, inplace=True)
print(df.head())
df.to_parquet('./data/prec.pq')

df = pd.read_csv("./data/100089.csv", sep=';')
df = df[['Zeitstempel','Abflussmenge']]

df['Zeitstempel']=pd.to_datetime(df['Zeitstempel'], utc=True)
df['datum'] = pd.to_datetime(df['Zeitstempel']).dt.date
df = df.groupby(['datum'])['Abflussmenge'].agg(['mean']).reset_index()
df.rename(columns = {'mean': 'abfluss'}, inplace=True)
print(df.head())
df.to_parquet('./data/pegel.pq')