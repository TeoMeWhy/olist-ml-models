# Databricks notebook source
# MAGIC %pip install feature-engine

# COMMAND ----------

from sklearn import model_selection
from sklearn import tree
from sklearn import pipeline
import pandas as pd

from feature_engine import imputation

pd.set_option('display.max_rows', 1000)

# COMMAND ----------

# DBTITLE 1,Sample Out Of Time
## SAMPLE

df = spark.table("silver.analytics.abt_olist_churn").toPandas()

# Base Out Of Time
df_oot = df[df['dtReference']=='2018-01-01']

# Base de Treino
df_train = df[df['dtReference']!='2018-01-01']

# COMMAND ----------

# DBTITLE 1,Definindo variáveis
df_train.head()

var_identity = ['dtReference','idVendedor']
target = 'flChurn'

features = df.columns.tolist()
features = list(set(features) - set(var_identity + [target]))
features.sort()

# COMMAND ----------

X_train, X_test, y_train, y_test = model_selection.train_test_split(df_train[features],
                                                                    df_train[target],
                                                                    train_size=0.8,
                                                                    random_state=42)

print("Proporção resposta Treino:", y_train.mean())
print("Proporção resposta Teste:", y_test.mean())

# COMMAND ----------

# DBTITLE 1,Explore
X_train.isna().sum().sort_values(ascending=False)

missing_minus_100 = ['avgIntervaloVendas',
                     'maxNota',
                     'medianNota',
                     'minNota',
                     'avgNota',
                     'avgVolumeProduto',
                     'minVolumeProduto',
                     'maxVolumeProduto',
                     'medianVolumeProduto',
                    ]

missing_0 = ['medianQtdeParcelas',
             'avgQtdeParcelas',
             'minQtdeParcelas',
             'maxQtdeParcelas',
            ]

# COMMAND ----------

# DBTITLE 1,Transform
imputer_minus_100 = imputation.ArbitraryNumberImputer(arbitrary_number=-100,
                                                      variables=missing_minus_100)

imputer_0 = imputation.ArbitraryNumberImputer(arbitrary_number=0,
                                              variables=missing_0)

# COMMAND ----------

# DBTITLE 1,Model
# este é um modelo de árvore de decisão
model = tree.DecisionTreeClassifier()

# COMMAND ----------

model_pipeline = pipeline.Pipeline([("Imputer -100", imputer_minus_100),
                                    ("Imputer 0", imputer_0),
                                    ("Decision Tree", model),
                                    ])

# COMMAND ----------

model_pipeline.fit(X_train, y_train)

# COMMAND ----------

model_pipeline.predict(X_test)
