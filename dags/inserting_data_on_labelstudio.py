from airflow import DAG
from datetime import datetime
from airflow.operators.python import PythonOperator
import mysql.connector
import requests
import json

def inserting_data_on_labelstudio():

    MYSQL_CONFIG = {
        'host': 'mysql',
        'user': 'root',
        'password': 'rootpassword',
        'database': 'selic_data'
    }

    LABEL_STUDIO_URL = "http://labelstudio:8080"
    API_TOKEN = "4492ce19e9ac04d1f7425b8cdbd8c629120bf494"
    PROJECT_ID = 1
    #
    conn = mysql.connector.connect(**MYSQL_CONFIG)
    cursor = conn.cursor(dictionary=True)

    cursor.execute(f"SELECT valor FROM data")
    rows = cursor.fetchall()

    tasks = []
    for row in rows:
        text_data = " | ".join([f"{k}: {v}" for k, v in row.items()])
        task = {
            "data": {
                "text": text_data
            }
        }
        tasks.append(task)

    # ==== ENVIO PARA LABEL STUDIO ====
    url = f"{LABEL_STUDIO_URL}/api/projects/{PROJECT_ID}/import"
    headers = {
        "Authorization": f"Token {API_TOKEN}",
        "Content-Type": "application/json"
    }

    response = requests.post(url, headers=headers, data=json.dumps(tasks))

    if response.status_code == 201:
        print(f"🚀 {len(tasks)} tarefas enviadas com sucesso para o Label Studio!")
    else:
        print(f"❌ Erro ao enviar tarefas: {response.status_code} - {response.text}")

    # ==== FECHAMENTO ====
    cursor.close()
    conn.close()

    label_config = """
    <View>
      <Text name="text" value="$text"/>
      <Choices name="category" toName="text">
        <Choice value="Acima da Meta"/>
        <Choice value="Abaixo da Meta"/>
      </Choices>
    </View>
    """
    url = f"{LABEL_STUDIO_URL}/api/projects/{PROJECT_ID}/"

    headers = {
        "Authorization": f"Token {API_TOKEN}",
        "Content-Type": "application/json",
    }

    data = {
        "label_config": label_config
    }

    requests.patch(url, headers=headers, json=data)


with DAG(
        dag_id='inserting_data_on_labelstudio_dag',
        start_date=datetime(2025, 1, 1),
        schedule_interval='@monthly',
        catchup=False
     ) as dag:
        input_data = PythonOperator(
            task_id='inserting_data_on_labelstudio',
            python_callable=inserting_data_on_labelstudio
        )