import pandas as pd

df = pd.read_csv("./data/100051.csv", sep=';')
df = df[['Datum/Zeit','PREC [mm]']]
df.rename(columns = {'Datum/Zeit':'zeit', 
    'PREC [mm]': 'prec', 
    }, inplace=True)

df['zeit']=pd.to_datetime(df['zeit'])
df['datum'] = pd.to_datetime(df['zeit']).dt.date
print(df.head())
df = df.groupby(['datum'])['prec'].agg(['sum']).reset_index()
df.to_parquet('./data/prec.pq')

df = pd.read_csv("./data/100089.csv", sep=';')
df = df[['Zeitstempel','Abflussmenge','Pegel']]
df.rename(columns = {'Zeitstempel':'zeit', 'Pegel': 'pegel', 'Abflussmenge': 'abflussmenge'}, inplace=True)

df['zeit']=pd.to_datetime(df['zeit'], utc=True)
df['datum'] = pd.to_datetime(df['zeit']).dt.date
df = df.groupby(['datum'])['pegel','abflussmenge'].agg(['mean']).reset_index()
df.columns = df.columns.droplevel(1)
df.to_parquet('./data/pegel.pq')





