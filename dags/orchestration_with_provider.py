from airflow import DAG
from airflow.providers.airbyte.operators.airbyte import AirbyteTriggerSyncOperator
from airflow.providers.airbyte.sensors.airbyte import AirbyteJobSensor
from airflow.providers.amazon.aws.operators.lambda_function import LambdaInvokeFunctionOperator
from airflow.models import Variable
from datetime import datetime, timedelta
import pendulum

# Configuration Airbyte et AWS
AIRBYTE_CONNECTION_IDS = {
    "s3": Variable.get("AIRBYTE_S3_CONNECTION_ID"),
    "judge_me": Variable.get("AIRBYTE_JUDGE_ME_CONNECTION_ID")
}
LAMBDA_FUNCTION_NAME = "dbt_runner_lambda"

# DAG definition
with DAG(
    dag_id='orchestration_with_provider_airbyte',
    default_args={'owner': 'airflow', 'retries': 1, 'retry_delay': timedelta(minutes=5)},
    schedule='@daily',
    start_date=pendulum.today('UTC').add(days=-1),
    catchup=False,
) as dag:

    # Tâche 1 : Déclencher la synchronisation Airbyte pour la source S3
    trigger_sync_s3 = AirbyteTriggerSyncOperator(
        task_id='start_airbyte_sync_s3',
        airbyte_conn_id='airbyte-api-cloud-connection',  # Nom de la connexion Airbyte configurée dans Airflow
        connection_id=AIRBYTE_CONNECTION_IDS["s3"],
        asynchronous=True,  # Lance la tâche sans attendre qu'elle se termine
        timeout=3600,  # Temps d'attente maximal pour la tâche, en secondes
        wait_seconds=3  # Temps à attendre entre chaque requête de statut
    )

    # Tâche 2 : Attendre que la synchronisation pour S3 soit terminée
    wait_for_sync_s3 = AirbyteJobSensor(
        task_id='wait_for_airbyte_sync_s3',
        airbyte_conn_id='airbyte-api-cloud-connection',
        airbyte_job_id="{{ task_instance.xcom_pull(task_ids='start_airbyte_sync_s3', key='return_value') }}",  # Utilise le job_id de la synchronisation S3
        timeout=3600,
        poke_interval=10  # Vérifie l'état du job toutes les 10 secondes
    )

    # Tâche 3 : Déclencher la synchronisation Airbyte pour la source Judge.me
    trigger_sync_judge_me = AirbyteTriggerSyncOperator(
        task_id='start_airbyte_sync_judge_me',
        airbyte_conn_id='airbyte-api-cloud-connection',
        connection_id=AIRBYTE_CONNECTION_IDS["judge_me"],
        asynchronous=True,
        timeout=3600,
        wait_seconds=3
    )

    # Tâche 4 : Attendre que la synchronisation pour Judge.me soit terminée
    wait_for_sync_judge_me = AirbyteJobSensor(
        task_id='wait_for_airbyte_sync_judge_me',
        airbyte_conn_id='airbyte-api-cloud-connection',
        airbyte_job_id="{{ task_instance.xcom_pull(task_ids='start_airbyte_sync_judge_me', key='return_value') }}",  # Utilise le job_id de la synchronisation Judge.me
        timeout=3600,
        poke_interval=10
    )

    # Tâche 5 : Appel de la fonction Lambda pour exécuter DBT après les synchronisations Airbyte
    lambda_invoke_dbt = LambdaInvokeFunctionOperator(
        task_id="lambda_invoke_dbt",
        function_name=LAMBDA_FUNCTION_NAME,
        aws_conn_id="aws_lambda_conn"  # Remplacez par le nom de votre connexion AWS dans Airflow
    )

    # Définir l'ordre des tâches
    trigger_sync_s3 >> wait_for_sync_s3
    trigger_sync_judge_me >> wait_for_sync_judge_me
    [wait_for_sync_s3, wait_for_sync_judge_me] >> lambda_invoke_dbt
