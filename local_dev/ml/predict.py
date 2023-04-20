# Databricks notebook source
# MAGIC %pip install feature-engine scikit-plot mlflow

# COMMAND ----------

import mlflow
import datetime

# COMMAND ----------

# DBTITLE 1,Predição
model = mlflow.sklearn.load_model("models:/Olist Vendedor Churn/Production")
df = spark.table("silver.analytics.fs_join").toPandas()
predict = model.predict_proba(df[model.feature_names_in_])

# COMMAND ----------

# DBTITLE 1,ETL
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

df_spark = spark.createDataFrame(df_extract)
df_spark.display()

# COMMAND ----------

def table_exists(database, table):
    count = (spark.sql(f"SHOW TABLES FROM {database}")
                  .filter(f"tableName = '{table}'")
                  .count())
    return count > 0

table = 'silver.analytics.olist_models'

if not table_exists('silver.analytics', 'olist_models'):
    print("Criando a tabela...")
    (df_spark.coalesce(1)
          .write
          .format("delta")
          .mode("overwrite")
          .option("overwriteSchema", "true")
          .partitionBy(["dtScore", "descModel"])
          .saveAsTable(table)
    )
    print("ok")
    
else:
    print("Atualizando dados...")
    spark.sql(f"DELETE FROM {table} WHERE dtScore = '{df['dtReference'][0]}' and descModel = 'Churn Vendedor'")
    (df_spark.coalesce(1)
              .write
              .format("delta")
              .mode("append")
              .saveAsTable(table))
