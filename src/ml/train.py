# Databricks notebook source
# MAGIC %pip install feature-engine scikit-plot mlflow

# COMMAND ----------

from sklearn import model_selection
from sklearn import tree
from sklearn import pipeline
from sklearn import metrics
from sklearn import ensemble

import scikitplot as skplt

import mlflow

from feature_engine import imputation

import pandas as pd

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
to_remove = ['qtdRecencia', target] + var_identity

features = df.columns.tolist()
features = list(set(features) - set(to_remove))
features.sort()
features

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

# DBTITLE 1,Define Experimento
mlflow.set_experiment("/Users/teomewhy@gmail.com/olist-churn-teo")

# COMMAND ----------

# DBTITLE 1,Model
with mlflow.start_run():

    mlflow.sklearn.autolog()
    mlflow.autolog()
    
    imputer_minus_100 = imputation.ArbitraryNumberImputer(arbitrary_number=-100,
                                                          variables=missing_minus_100)

    imputer_0 = imputation.ArbitraryNumberImputer(arbitrary_number=0,
                                                  variables=missing_0)

    # este é um modelo de árvore de decisão
    model = ensemble.RandomForestClassifier(n_jobs=-1,
                                            random_state=42)

    params = {"min_samples_leaf": [5,10,20],
              "n_estimators":[300,400,450, 500]}
    
    grid = model_selection.GridSearchCV(model, params, cv=3, verbose=3, scoring='roc_auc')
    
    # Criando o pipeline
    model_pipeline = pipeline.Pipeline([("Imputer -100", imputer_minus_100),
                                        ("Imputer 0", imputer_0),
                                        ("Grid Search", grid),
                                        ])  
   
    model_pipeline.fit(X_train, y_train)
    
    auc_train = metrics.roc_auc_score(y_train, model_pipeline.predict_proba(X_train)[:,1])
    auc_test = metrics.roc_auc_score(y_test, model_pipeline.predict_proba(X_test)[:,1])
    auc_oot = metrics.roc_auc_score(df_oot[target], model_pipeline.predict_proba(df_oot[features])[:,1])
    
    metrics_model = {"auc_train": auc_train,
                    "auc_test": auc_test,
                    "auc_oot": auc_oot}

    mlflow.log_metrics(metrics_model)

# COMMAND ----------

pd.DataFrame(grid.cv_results_).sort_values(by='rank_test_score')

# COMMAND ----------

grid.best_estimator_

# COMMAND ----------

skplt.metrics.plot_roc(y_train, probas)

# COMMAND ----------

skplt.metrics.plot_ks_statistic(y_train, probas)

# COMMAND ----------

skplt.metrics.plot_lift_curve(y_train, probas)

# COMMAND ----------

df_teo = pd.DataFrame()
df_teo['target'] = y_train
df_teo['proba'] = probas[:,1]
df_teo = df_teo.sort_values(by='proba')
df_teo.tail(100)['target'].mean() / df_teo['target'].mean()

# COMMAND ----------

probas_test = model_pipeline.predict_proba(X_test)

# COMMAND ----------

skplt.metrics.plot_roc(y_test, probas_test)

# COMMAND ----------

skplt.metrics.plot_ks_statistic(y_test, probas_test)

# COMMAND ----------

probas_oot = model_pipeline.predict_proba(df_oot[features])

# COMMAND ----------

skplt.metrics.plot_roc(df_oot[target], probas_oot)

# COMMAND ----------

skplt.metrics.plot_ks_statistic(df_oot[target], probas_oot)

# COMMAND ----------

fs_importance = model_pipeline[-1].feature_importances_
fs_cols = model_pipeline[:-1].transform(X_train.head(1)).columns.tolist()

pd.Series(fs_importance, index=fs_cols).sort_values(ascending=False)
