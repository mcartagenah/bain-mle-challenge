from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import logging
from numpy import random
import pandas as pd

from helpers import data_helper, training_helper

logging.basicConfig(level=logging.WARN)
logger = logging.getLogger(__name__)

def data_preprocessing_milk():
    """
        This method processes the milk data.
    """
    try:
        logger.info('Preprocessing milk data')
        data_helper.process_milk_data()

    except Exception as err:
        logger.exception(err)
        raise err

def data_preprocessing_bank():
    """
        This method processes the bank data.
    """
    try:
        logger.info('Preprocessing bank data')
        data_helper.process_bank_data()

    except Exception as err:
        logger.exception(err)
        raise err

def data_preprocessing_rain():
    """
        This method processes the rain data.
    """
    try:
        logger.info('Preprocessing rain data')
        data_helper.process_rain_data()

    except Exception as err:
        logger.exception(err)
        raise err

def feature_engineering():
    """
        This method produces new features from the already processed milk, bank and rain data.
    """
    try:
        logger.info('Feature engineering')
        data_helper.feature_engineering()

    except Exception as err:
        logger.exception(err)
        raise err

with DAG(dag_id='data_processing_dag', start_date=datetime(2022, 1, 1), 
    schedule_interval='@daily', catchup=False) as dag:

    t1 = PythonOperator(
        task_id='t1',
        op_kwargs=dag.default_args,
        provide_context=True,
        python_callable=data_preprocessing_milk
    )

    t2 = PythonOperator(
        task_id='t2',
        op_kwargs=dag.default_args,
        provide_context=True,
        python_callable=data_preprocessing_rain
    )

    t3 = PythonOperator(
        task_id='t3',
        op_kwargs=dag.default_args,
        provide_context=True,
        python_callable=data_preprocessing_bank
    )

    t4 = PythonOperator(
        task_id='t4',
        op_kwargs=dag.default_args,
        provide_context=True,
        python_callable=feature_engineering
    )


    [t1, t2, t3] >> t4
