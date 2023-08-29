# Databricks notebook source
import datetime 

def import_query(path): 
    with open (path, 'r') as open_file: 
        return open_file.read() 
    
def table_exists (database, table): 
    count = (spark.sql('SHOW TABLES FROM {}'.format(database))
             .filter("table_name = {}".format(table))
             .count())
    return count>0 

def date_range (dt_start, dt_stop, period='daily'):
    datetime_start = datetime.datetime.strptime (dt_start, '%Y-%m-%d')
    datetime_stop = datetime. datetime.strptime (dt_stop, '%Y-%m-%d')
    dates = []
    while datetime_start <= datetime_stop:
        dates.append(datetime_start.strftime ("%Y-%m-%d" ))
        datetime_start += datetime.timedelta (days=1)
    if period == 'daily':
        return dates 
    elif period =='monthly':
        return [i for i in dates if i.endswith("01")]


# COMMAND ----------

import datetime
from tqdm import tqdm

def import_query(path):
    with open(path, 'r') as open_file:
        return open_file.read()


def table_exists(database, table):
    count = (spark.sql(f"SHOW TABLES FROM {database}")
                  .filter(f"tableName = '{table}'")
                  .count())
    return count > 0


def date_range(dt_start, dt_stop, period='daily'):
    datetime_start = datetime.datetime.strptime(dt_start, '%Y-%m-%d')
    datetime_stop = datetime.datetime.strptime(dt_stop, '%Y-%m-%d')
    dates = []
    while datetime_start <= datetime_stop:
        dates.append(datetime_start.strftime("%Y-%m-%d"))
        datetime_start += datetime.timedelta(days=1)
    if period == 'daily':
        return dates
    elif period == 'monthly':
        return [i for i in dates if i.endswith("01")]

table = dbutils.widgets.get("table")
table_name = f"fs_vendedor_{table}"
database = "silver.analytics"
period = dbutils.widgets.get("period")

query = import_query(f"{table}.sql")

date_start = dbutils.widgets.get("date_start")
date_stop = dbutils.widgets.get("date_stop")

dates = date_range(date_start, date_stop, period)

print(table_name, table_exists(database, table_name))
print(date_start, ' ~ ', date_stop)

# COMMAND ----------

if not table_exists(database, table_name):
    print("Criando a tabela...")
    (spark.sql(query.format(date=dates.pop(0)))
          .coalesce(1)
          .write
          .format("delta")
          .mode("overwrite")
          .option("overwriteSchema", "true")
          .partitionBy("dtReference")
          .saveAsTable(f"{database}.{table_name}")
    )
    print("ok")

print("Realizando update")
for i in tqdm(dates):
    spark.sql(f"DELETE FROM {database}.{table_name} WHERE dtReference = '{i}'")
    (spark.sql(query.format(date=i))
          .coalesce(1)
          .write
          .format("delta")
          .mode("append")
          .saveAsTable(f"{database}.{table_name}"))
print("ok")

# COMMAND ----------

date_start = '2017-01-01'
date_end = '2018-01-01' 
date_range(date_start, date_end, 'monthly')

# COMMAND ----------

table = 'vendas'
table_name = 'fs_vendedor_{}'.format(table) 
database = 'silver.analytics'


# COMMAND ----------

print(table_exists(database, table))

# COMMAND ----------

query = import_query('vendas.sql')

# COMMAND ----------

df.display()

