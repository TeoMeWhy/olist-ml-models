import os
import datetime
import sqlalchemy
import pandas as pd


ML_DIR = os.path.dirname(os.path.abspath(__file__))
LOCAL_DEV_DIR = os.path.dirname(ML_DIR)
ROOT_DIR = os.path.dirname(LOCAL_DEV_DIR)
DATA_DIR = os.path.join(ROOT_DIR, 'data')
DB_PATH = os.path.join(DATA_DIR, 'olist.db')
MODEL_DIR = os.path.join(ROOT_DIR, 'models')

model = pd.read_pickle(f"{MODEL_DIR}/churn_olist_lgbm.pkl")

engine = sqlalchemy.create_engine(f"sqlite:///{DB_PATH}")

with engine.connect() as con:
    df = pd.read_sql_table("fs_join", con)

predict = model['model'].predict_proba(df[model['features']])

predict_0 = predict[:,0]
predict_1 = predict[:,1]

df_extract = df[['idVendedor']].copy()
df_extract['0'] = predict_0
df_extract['1'] = predict_1

df_extract = (df_extract.set_index('idVendedor')
                        .stack()
                        .reset_index())

df_extract.columns = ['idVendedor','descClass', 'Score']
df_extract['descModel'] = 'Churn Vendedor'
dt_now = datetime.datetime.now().strftime("%Y-%m-%d")
df_extract['dtScore'] = df['dtReference'][0]
df_extract['dtIngestion'] = dt_now

print(df_extract)