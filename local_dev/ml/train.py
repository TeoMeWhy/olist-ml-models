import os
import sqlalchemy

from sklearn import model_selection
from sklearn import tree
from sklearn import pipeline
from sklearn import metrics
from sklearn import ensemble
import lightgbm

from feature_engine import imputation

import scikitplot as skplt

import pandas as pd

ML_DIR = os.path.dirname(os.path.abspath(__file__))
LOCAL_DEV_DIR = os.path.dirname(ML_DIR)
ROOT_DIR = os.path.dirname(LOCAL_DEV_DIR)
DATA_DIR = os.path.join(ROOT_DIR, 'data')
DB_PATH = os.path.join(DATA_DIR, 'olist.db')
MODEL_DIR = os.path.join(ROOT_DIR, 'models')

engine = sqlalchemy.create_engine(f"sqlite:///{DB_PATH}")

with engine.connect() as con:
    df = pd.read_sql_table("abt_olist_churn", con)


# Base Out Of Time
df_oot = df[df['dtReference']>='2018-01-01']

# Base de Treino
df_train = df[df['dtReference']<'2018-01-01']

var_identity = ['dtReference','idVendedor']
target = 'flChurn'
to_remove = ['qtdRecencia', target] + var_identity

features = df.columns.tolist()
features = list(set(features) - set(to_remove))
features.sort()
features

X_train, X_test, y_train, y_test = model_selection.train_test_split(df_train[features],
                                                                    df_train[target],
                                                                    train_size=0.8,
                                                                    random_state=42)

print("Proporção resposta Treino:", y_train.mean())
print("Proporção resposta Teste:", y_test.mean())


# Explor
X_train.isna().sum().sort_values(ascending=False)

missing_minus_100 = ['avgIntervaloVendas',
                     'maxNota',
                     'minNota',
                     'avgNota',
                     'avgVolumeProduto',
                     'minVolumeProduto',
                     'maxVolumeProduto',
                    ]

missing_0 = [
             'avgQtdeParcelas',
             'minQtdeParcelas',
             'maxQtdeParcelas',
            ]

imputer_minus_100 = imputation.ArbitraryNumberImputer(arbitrary_number=-100,
                                                        variables=missing_minus_100)

imputer_0 = imputation.ArbitraryNumberImputer(arbitrary_number=0,
                                                variables=missing_0)

# este é um modelo de árvore de decisão
model = lightgbm.LGBMClassifier(n_jobs=-1,
                                learning_rate=0.1,
                                min_child_samples=30,
                                max_depth=10,
                                n_estimators=400)

#params = {"learning_rate": [0.1, 0.5, 0.7, 0.9, 0.99999],
#          "n_estimators":[300,400,450, 500],
#          "min_child_samples": [20,30,40,50,100]             
#         }

#grid = model_selection.GridSearchCV(model, params, cv=3, verbose=3, scoring='roc_auc')

# Criando o pipeline
model_pipeline = pipeline.Pipeline([("Imputer -100", imputer_minus_100),
                                    ("Imputer 0", imputer_0),
                                    #("Grid Search", grid),
                                    ("LGBM Model", model),
                                    ])  

model_pipeline.fit(X_train, y_train)

auc_train = metrics.roc_auc_score(y_train, model_pipeline.predict_proba(X_train)[:,1])
auc_test = metrics.roc_auc_score(y_test, model_pipeline.predict_proba(X_test)[:,1])
auc_oot = metrics.roc_auc_score(df_oot[target], model_pipeline.predict_proba(df_oot[features])[:,1])

metrics_model = {"auc_train": auc_train,
                "auc_test": auc_test,
                "auc_oot": auc_oot}

print(metrics_model)

model_dict = {"model": model_pipeline,
              "features": X_train.columns.tolist()}

model_dict.update(metrics_model)

model_results = pd.Series(model_dict)
model_results.to_pickle(f"{MODEL_DIR}/churn_olist_lgbm.pkl")