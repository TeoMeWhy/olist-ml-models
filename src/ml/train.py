# Databricks notebook source
# MAGIC %md ## Bibliotecas

# COMMAND ----------

# MAGIC %pip install 

# COMMAND ----------

# MAGIC %pip install feature-engine scikit-plot mlflow

# COMMAND ----------

from sklearn import model_selection
from sklearn import tree
from sklearn import metrics
import pandas as pd
from feature_engine import imputation
from sklearn import pipeline

# para trabalhar com métricas
import scikitplot as skplt

import mlflow

import random

pd.set_option('display.max_rows', 1000)

# COMMAND ----------

# MAGIC %md ##Sample

# COMMAND ----------

df = spark.table("silver.analytics.abt_olist_churn").toPandas()


# COMMAND ----------

# MAGIC %md ### Separar a base em dois:
# MAGIC 
# MAGIC 1. Out of time - base temporalmente deslocada = M+1
# MAGIC 2. Treino - 2017 até 2018

# COMMAND ----------

# Base Out of time (OOT)

df_oot = df[df['dtReference'] == '2018-01-01']
df_oot.shape

#Base treino

df_train = df[df['dtReference'] != '2018-01-01']
df_train.shape

# COMMAND ----------

# MAGIC %md ## Definindo Variáveis

# COMMAND ----------

df_train.head()

# Variáveis de identificação

var_identity = ['dtReference', 'idVendedor']

# Variáveis respossta
target = 'flChurn'

# removendo a variável de recencia, pois ela era criada majotariamente por ela
to_remove = ['qtdRecencia', target] + var_identity

features = df.columns.tolist()
features = list(set(features) - set(to_remove))

features.sort()

# COMMAND ----------

# MAGIC %md ## Separando base treino e base teste
# MAGIC Usando  a model_selection

# COMMAND ----------

x_train, x_teste, y_train, y_teste = model_selection.train_test_split(df_train[features], 
                                                                      df_train[target],
                                                                      train_size = 0.8, # tamanho da divisao - 80% para treino e 20% para teste
                                                                      random_state = 42) #seed

print("Proporcao resposta Treino:", y_train.mean())
print("Proporcao resposta Teste:", y_teste.mean())

# OBS: os dados não estão balanceados!!!!!

# COMMAND ----------

# MAGIC %md ## Explore
# MAGIC Variáveis com dados faltantes

# COMMAND ----------

#x_train.describe() # Identificando valores nulos

#x_train.isna().sum().sort_values(ascending=False)

# Atribuir -100 aos dados faltantes

missing_minus_100 = ['avgIntervaloVendas',
                     'maxNota',                                 
                     'medianNota',                              
                     'minNota',                                 
                     'avgNota',
                     'avgVolumeProduto',                       
                     'minVolumeProduto',                       
                     'maxVolumeProduto',                       
                     'medianVolumeProduto'
                  ]
# Atribuir 0 aos dados faltantes

missing_0 = ['medianQtdeParcelas',                    
             'avgQtdeParcelas',                       
             'minQtdeParcelas',                       
             'maxQtdeParcelas'
            ]


# COMMAND ----------

with mlflow.start_run():
    
    mlflow.sklearn.autolog()
    
    imputer_minus_100 = imputation.ArbitraryNumberImputer(arbitrary_number = -100,
                                                      variables = missing_minus_100)

    imputer_0 = imputation.ArbitraryNumberImputer(arbitrary_number = 0,
                                                      variables = missing_0)
    
    model = tree.DecisionTreeClassifier(min_samples_leaf = 50)
    
    model_pipeline = pipeline.Pipeline([('imputer -100', imputer_minus_100),
                                          ("imputer 0", imputer_0),
                                           ("Decision Tree", model)
                                       ])
    
    model_pipeline.fit(x_train, y_train)


    auc_train = metrics.roc_auc_score(y_train, model_pipeline.predict_proba(x_train)[:,1])
    auc_teste = metrics.roc_auc_score(x_teste, model_pipeline.predict_proba(x_teste)[:,1])
    auc_oot = metrics.roc_auc_score(df_oot[target], model_pipeline.predict_proba(df_oot[features][:,1]))

    metrics_model = {"auc_train": auc_train,
                     "auc_teste": auc_teste,
                     "auc_oot": auc_oot}
    
    mlflow.log_metrics(metrics_model)

# COMMAND ----------

# MAGIC %md ## Define experimento

# COMMAND ----------

mlflow.set_experiment('/Users/andrealonso75@gmail.com/olist-churn-andreaa')

# COMMAND ----------

# MAGIC %md ## Transform
# MAGIC Imputadores de dados

# COMMAND ----------

imputer_minus_100 = imputation.ArbitraryNumberImputer(arbitrary_number = -100,
                                                      variables = missing_minus_100)

imputer_0 = imputation.ArbitraryNumberImputer(arbitrary_number = 0,
                                                      variables = missing_0)

x_new = imputer_minus_100.fit_transform(x_train)
x_new = imputer_0.fit_transform(x_new)

# COMMAND ----------

# MAGIC %md ## Modeling - Modelo de árvore de decisão
# MAGIC 
# MAGIC Classificando se é Churn ou não Churn

# COMMAND ----------

model = tree.DecisionTreeClassifier(min_samples_leaf = 50)
model.fit(x_new, y_train)

# Ao rodar aqui primeiro gerou um erro pois a base contém valores como NaN
# Para arrumar isso vai ter que ser feito a parte descritiva (Explore) utilizando a base treino

# COMMAND ----------

# MAGIC %md ## Criando um Pipeline 

# COMMAND ----------



model_pipeline = pipeline.Pipeline([('imputer -100', imputer_minus_100),
                                     ("imputer 0", imputer_0),
                                       ("Decision Tree", model)
                                   ]
                                  )



# COMMAND ----------

# MAGIC %md ### Treino do algoritimo

# COMMAND ----------

model_pipeline.fit(x_train, y_train)

# COMMAND ----------

predict = model_pipeline.predict(x_train)

probas = model_pipeline.predict_proba(x_train)

proba = probas[:,1]
proba

# Teste de uma curva ruim

proba_ruim = [[0.5, 0.5] for i in predict]

# COMMAND ----------

# MAGIC %md ## Curva ROC
# MAGIC As curvas ROC descrevem a capacidade discriminativa de um teste diagnóstico para um determinado número de valores "cutoff point". Isto permite pôr em evidência os valores para os quais existe maior optimização da sensibilidade em função da especificidade.
# MAGIC Sendo assim, a curva tem que ester distante do eixo do meio (linha reta y = x)

# COMMAND ----------

# MAGIC %md ### Curva boa

# COMMAND ----------

skplt.metrics.plot_roc(y_train, probas)

# COMMAND ----------

# MAGIC %md ### Curva ruim - Exemplo

# COMMAND ----------

skplt.metrics.plot_roc(y_train, proba_ruim)

# todos os resultados tem 0.5

# COMMAND ----------

skplt.metrics.plot_ks_statistic(y_train, probas)

# COMMAND ----------

# MAGIC %md ks é a distancia entre os pontos. Isso significa que o corte é 0.324

# COMMAND ----------

proba.mean()

# COMMAND ----------

df_score = pd.DataFrame()
df_score['flChurn'] = y_train
df_score['proba'] = proba
spark.createDataFrame(df_score.reset_index().head(1000)).display()

# COMMAND ----------

probas_test = model_pipeline.predict_proba(x_teste)


# COMMAND ----------

skplt.metrics.plot_roc(y_teste, probas_test)

# COMMAND ----------

skplt.metrics.plot_ks_statistic(y_teste, probas_test)

# COMMAND ----------

probas_oot = model_pipeline.predict_proba(df_oot[features])

# COMMAND ----------

skplt.metrics.plot_roc(df_oot[target], probas_oot)

# COMMAND ----------

skplt.metrics.plot_ks_statistic(df_oot[target], probas_oot)

# COMMAND ----------

# MAGIC %md ### Definindo as variáveis importantes para o modelo

# COMMAND ----------

fs_importance = model_pipeline[-1].feature_importances_

fs_cols = model_pipeline[:-1].transform(x_train.head(1)).columns.tolist()

pd.Series(fs_importance, index = fs_cols).sort_values(ascending = False)

# COMMAND ----------

# MAGIC %md ### Lift Curve

# COMMAND ----------

skplt.metrics.plot_lift_curve(y_train, probas);
