from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import logging

from helpers import training_helper

logging.basicConfig(level=logging.WARN)
logger = logging.getLogger(__name__)

def generate_train_test():
    """
        This method separates the data into train and test datasets.
    """
    try:
        logger.info('Generate Train/Test')
        training_helper.generate_train_test()

    except Exception as err:
        logger.exception(err)
        raise err

def training():
    """
        This method does a grid search to find the best hyperparameters for the model.
    """
    try:
        logger.info('Model training')
        training_helper.grid_search_training()

    except Exception as err:
        logger.exception(err)
        raise err

with DAG(dag_id='training_dag', start_date=datetime(2022, 1, 1), 
    schedule_interval='@daily', catchup=False) as dag:

    t1 = PythonOperator(
        task_id='t1',
        op_kwargs=dag.default_args,
        provide_context=True,
        python_callable=generate_train_test
    )

    t2 = PythonOperator(
        task_id='t2',
        op_kwargs=dag.default_args,
        provide_context=True,
        python_callable=training
    )

    t1 >> t2
