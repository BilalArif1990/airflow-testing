##Step 1: Import Basics
import airflow
from airflow import DAG
from airflow.operators.python import PythonOperators, BranchPythonOperator
from airflow.hooks import BashOperator
from datetime import datetime
from random import randint

def _training_model():
    return randint(1,10)

def _choose_best_model(ti):
    accuracies = ti.xcom_pull(task_ids=['training_model_A','training_model_B','training_model_C'])
    best_accuracy = max(accuracies)
    if(best_accuracy > 8):
        return 'accurate'
    return 'inaccurate'

## Create Instance
## dag name, schedule date(start_date), schedule_interval, 
with DAG("my_dag", start_date=datetime(2021,1,1),schedule_interval="@daily", catchup=False) as dag:

    training_model_A = PythonOperators(
        task_id="training_model_A",
        python_callable=_training_model
    )

    training_model_B = PythonOperators(
        task_id="training_model_B",
        python_callable=_training_model
    )

    
    training_model_C = PythonOperators(
        task_id="training_model_C",
        python_callable=_training_model
    )

    choose_best_model = BranchPythonOperator(
        task_id="choose_best_model",
        pythin_callable=_choose_best_model
    )

    accurate = BashOperator(
        task_id="accurate",
        bash_command="echo 'accurate'"
    )

    inaccurate = BashOperator(
        task_id="inaccurate",
        bash_command="echo 'inaccurate'"
    )

