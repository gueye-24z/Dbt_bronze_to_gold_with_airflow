from airflow import DAG

from airflow.providers.http.operators.http import SimpleHttpOperator

from airflow.providers.http.sensors.http import HttpSensor

from airflow.providers.amazon.aws.operators.lambda_function import LambdaInvokeFunctionOperator

from airflow.operators.python_operator import PythonOperator

from airflow.models import Variable

import pendulum

import json

import requests


# Configuration Airbyte et AWS

AIRBYTE_CONNECTION_IDS = {

    "s3": Variable.get("AIRBYTE_S3_CONNECTION_ID"),

    "judge_me": Variable.get("AIRBYTE_JUDGE_ME_CONNECTION_ID")

}

AIRBYTE_CLIENT_ID = Variable.get("AIRBYTE_CLIENT_ID")

AIRBYTE_CLIENT_SECRET = Variable.get("AIRBYTE_CLIENT_SECRET")

LAMBDA_FUNCTION_NAME = "dbt_runner_lambda"


# Fonction pour générer un jeton d'API Airbyte

def generate_token():

    url = "https://api.airbyte.com/v1/applications/token"  # Assurez-vous de cette URL

    headers = {"Content-Type": "application/json"}

    data = {

        "client_id": AIRBYTE_CLIENT_ID,

        "client_secret": AIRBYTE_CLIENT_SECRET,

        "grant_type": "client_credentials"

    }

    response = requests.post(url, headers=headers, json=data)

    response.raise_for_status()

    token = response.json().get("access_token")

    if not token:

        raise ValueError("Échec de la génération du jeton")

    return token


# DAG definition

with DAG(

    dag_id='airbyte_lambda_dbt_pipeline',

    default_args={'owner': 'airflow'},

    schedule='@daily',

    start_date=pendulum.today('UTC').add(days=-1),

    catchup=False,

) as dag:


    # Tâche : Générer un jeton d'API

    generate_airbyte_token = PythonOperator(

        task_id="generate_airbyte_token",

        python_callable=generate_token,

        do_xcom_push=True

    )


    # Tâche 1 : Déclencher la synchronisation Airbyte pour la source S3

    trigger_sync_s3 = SimpleHttpOperator(

        method="POST",

        task_id='start_airbyte_sync_s3',

        http_conn_id='airbyte-api-cloud-connection',

        headers={

            "Content-Type": "application/json",

            "User-Agent": "fake-useragent",

            "Accept": "application/json",

            "Authorization": "Bearer {{ ti.xcom_pull(task_ids='generate_airbyte_token') }}"

        },

        endpoint='/v1/jobs',

        data=json.dumps({"connectionId": AIRBYTE_CONNECTION_IDS["s3"], "jobType": "sync"}),

        do_xcom_push=True,

        response_filter=lambda response: response.json()['jobId'],

        log_response=True,

    )


    # Tâche 2 : Attendre que la synchronisation pour S3 soit terminée

    wait_for_sync_s3 = HttpSensor(

        method='GET',

        task_id='wait_for_airbyte_sync_s3',

        http_conn_id='airbyte-api-cloud-connection',

        headers={

            "Content-Type": "application/json",

            "User-Agent": "fake-useragent",

            "Accept": "application/json",

            "Authorization": "Bearer {{ ti.xcom_pull(task_ids='generate_airbyte_token') }}"

        },

        endpoint='/v1/jobs/{}'.format("{{ task_instance.xcom_pull(task_ids='start_airbyte_sync_s3') }}"),

        poke_interval=30,

        timeout=600,

        response_check=lambda response: json.loads(response.text)['status'] == "succeeded",

    )


    # Tâche 3 : Déclencher la synchronisation Airbyte pour la source Judge.me

    trigger_sync_judge_me = SimpleHttpOperator(

        method="POST",

        task_id='start_airbyte_sync_judge_me',

        http_conn_id='airbyte-api-cloud-connection',

        headers={

            "Content-Type": "application/json",

            "User-Agent": "fake-useragent",

            "Accept": "application/json",

            "Authorization": "Bearer {{ ti.xcom_pull(task_ids='generate_airbyte_token') }}"

        },

        endpoint='/v1/jobs',

        data=json.dumps({"connectionId": AIRBYTE_CONNECTION_IDS["judge_me"], "jobType": "sync"}),

        do_xcom_push=True,

        response_filter=lambda response: response.json()['jobId'],

        log_response=True,

    )


    # Tâche 4 : Attendre que la synchronisation pour Judge.me soit terminée

    wait_for_sync_judge_me = HttpSensor(

        method='GET',

        task_id='wait_for_airbyte_sync_judge_me',

        http_conn_id='airbyte-api-cloud-connection',

        headers={

            "Content-Type": "application/json",

            "User-Agent": "fake-useragent",

            "Accept": "application/json",

            "Authorization": "Bearer {{ ti.xcom_pull(task_ids='generate_airbyte_token') }}"

        },

        endpoint='/v1/jobs/{}'.format("{{ task_instance.xcom_pull(task_ids='start_airbyte_sync_judge_me') }}"),

        poke_interval=30,

        timeout=600,

        response_check=lambda response: json.loads(response.text)['status'] == "succeeded",

    )


    # Tâche 5 : Appel de la fonction Lambda pour exécuter DBT après les synchronisations Airbyte

    lambda_invoke_dbt = LambdaInvokeFunctionOperator(

        task_id="lambda_invoke_dbt",

        function_name=LAMBDA_FUNCTION_NAME,

        aws_conn_id="aws_lambda_conn"

    )


    # Définir l'ordre des tâches

    generate_airbyte_token >> trigger_sync_s3 >> wait_for_sync_s3

    generate_airbyte_token >> trigger_sync_judge_me >> wait_for_sync_judge_me

    [wait_for_sync_s3, wait_for_sync_judge_me] >> lambda_invoke_dbt
