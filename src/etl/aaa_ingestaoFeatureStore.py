# Databricks notebook source
#%pip install tqdm

# COMMAND ----------

# MAGIC %md ## Criando funcoes auxiliares

# COMMAND ----------

import datetime
from tqdm import tqdm # Estimativa de tempo para rodas a query

#------------------------------------------------------------------------

def import_query(path):
    """
    funcao para abrir um arquivo e importar o que esta dentro dele
    """
    with open(path,'r') as open_file:
        return open_file.read()
    
def table_exists(database, table):
    """
    Funcao que verifica se a tabela existe
    """
    count = (spark.sql(f"SHOW TABLES FROM {database}")
                  .filter(f"tablename = '{table}'")
                  .count())
    return count > 0

def date_range(dt_start, dt_stop, period = 'daily'):
    """
    Funcao que faz um range de datas
    """
    datetime_start = datetime.datetime.strptime(dt_start, '%Y-%m-%d')
    datetime_stop = datetime.datetime.strptime(dt_stop, '%Y-%m-%d')
    dates = []

    while datetime_start <= datetime_stop:
        dates.append(datetime_start.strftime("%Y-%m-%d"))
        datetime_start +=  datetime.timedelta(days = 1)
    if period == 'daily':
        return dates
    elif period == 'monthly':
        return [i for i in dates if i.endswith("01")]

table = dbutils.widget.get("table")
table_name = f"fs_vendedor_{table}"
database = "silver.analytics"
period = dbutils.widget.get("period")
    
date_start = dbutils.widget.get('date_start')
date_stop = dbutils.widget.get('date_stop')
dates = date_range(date_start, date_stop, period)

print(table_name, table_exists(database, table_name))
print(date_start, ' ~ ', date_stop)
 

# COMMAND ----------

if not table_exists(database, table_name):
    print("Criando a tabela...")
    df =(spark.sql(query.format(date = dates.pop(0)))
              .df.coalesce(1)
              .write
              .format("delta")
              .mode("overwrite")
              .option("overwriteSchema", "true")
              .partitionBy("dtReference")
              .saveAsTable(f"{database}.{table_name}")
        )
    print("ok")
    
else:
    print("Realizando update")
    for i in dates:
        spark.sql(f"DELETE FROM {database}.{table_name} WHERE dtReference = '{i}'")
        (spark.sql(query.format(date=i))
              .coalesce(1)
              .write
              .format("delta")
              .mode("append")
              .saveASTable(f"{database}.{table_name}"))
        print("ok")

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT * FROM silver.analytics.fs_vendedor_vendas
