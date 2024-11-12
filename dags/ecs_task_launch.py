from airflow import DAG
from airflow.providers.amazon.aws.operators.ecs import EcsRunTaskOperator
from datetime import datetime, timedelta

# Configuration du DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Remplissez les détails de votre cluster, tâche et configuration réseau
CLUSTER_NAME = 'cluster_docker'
TASK_DEFINITION = 'arn:aws:ecs:eu-west-3:442426866837:task-definition/dbt:2'  # Nom ou ARN de la définition de tâche ECS
CONTAINER_NAME = 'container_dbt'
ECR_IMAGE = '442426866837.dkr.ecr.eu-west-3.amazonaws.com/dbt_transform_comments:v0.1'  # URI complet de l'image ECR, avec le tag (par exemple, '123456789012.dkr.ecr.us-east-1.amazonaws.com/your-repo:latest')
SUBNETS = ['subnet-01fa8da38f6deab8e', 'subnet-08742c0c6afe3c769', 'subnet-036907fb914bd9159']
SECURITY_GROUPS = ['sg-0839cb2134a0c2e11']

# Création du DAG
with DAG(
    'ecs_docker_task_dag',
    default_args=default_args,
    description='Un DAG pour lancer une tâche ECS avec un conteneur Docker ECR',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 1, 1),
    catchup=False,
) as dag:

    # Définition de la tâche ECS avec EcsRunTaskOperator
    run_ecs_task = EcsRunTaskOperator(
        task_id='run_ecs_task',
        cluster=CLUSTER_NAME,
        task_definition=TASK_DEFINITION,
        launch_type='FARGATE',  # Utilisez 'EC2' si vous n'utilisez pas Fargate
        aws_conn_id='aws_conn',
        overrides={},
        network_configuration={
            'awsvpcConfiguration': {
                'subnets': SUBNETS,
                'securityGroups': SECURITY_GROUPS,
                'assignPublicIp': 'ENABLED',  # Activation de l'IP publique comme dans votre capture d'écran
            }
        },
        region_name='eu-west-3',  # Région de votre cluster ECS
        tags={'Project': 'MyProject', 'Environment': 'Production'},
    )

    # Définissez d'autres tâches si nécessaire et leurs dépendances
    run_ecs_task  # La tâche principale du DAG

