import pandas as pd
import numpy as np
from sklearn.model_selection import train_test_split
from sklearn.model_selection import GridSearchCV
from sklearn.preprocessing import StandardScaler, PolynomialFeatures
from sklearn.pipeline import Pipeline
from sklearn.linear_model import Ridge
from sklearn.feature_selection import SelectKBest, mutual_info_regression
import pickle

np.random.seed(0)

def generate_train_test():
    precio_leche_pp_pib = pd.read_csv('/opt/airflow/data/augmented_milk_data.csv')
    X = precio_leche_pp_pib.drop(['Precio_leche'], axis = 1)
    y = precio_leche_pp_pib['Precio_leche']
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)
    np.save('/opt/airflow/data/X_train.npy', X_train)
    np.save('/opt/airflow/data/y_train.npy', y_train)
    np.save('/opt/airflow/data/X_test.npy', X_test)
    np.save('/opt/airflow/data/y_test.npy', y_test)

def grid_search_training():
    X_train = np.load('/opt/airflow/data/X_train.npy')
    y_train = np.load('/opt/airflow/data/y_train.npy')
    X_test = np.load('/opt/airflow/data/X_test.npy')
    y_test = np.load('/opt/airflow/data/y_test.npy')
    pipe = Pipeline([('scale', StandardScaler()),
                     ('selector', SelectKBest(mutual_info_regression)),
                     ('poly', PolynomialFeatures()),
                     ('model', Ridge())])
    k=[3, 4, 5, 6, 7, 10]
    alpha=[1, 0.5, 0.2, 0.1, 0.05, 0.02, 0.01]
    poly = [1, 2, 3, 5, 7]
    grid = GridSearchCV(estimator = pipe,
                        param_grid = dict(selector__k=k,
                                          poly__degree=poly,
                                          model__alpha=alpha),
                        cv = 3, scoring = 'r2')
    grid.fit(X_train, y_train)
    with open('/opt/airflow/models/trained_model.pkl', 'wb') as f:
        pickle.dump(grid.best_estimator_, f)
