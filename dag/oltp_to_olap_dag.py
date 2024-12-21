from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.bash import BashOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
import os
import shutil
import logging
from airflow.operators.python import BranchPythonOperator
from airflow.operators.empty import EmptyOperator


# Default arguments untuk DAG
default_args = {
    'owner': 'man',
    'depends_on_past': False,
    'email': ['agipriono1410@gmail.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=1)
}

# Inisialisasi DAG
dag = DAG(
    'tugas_akhir',
    default_args=default_args,
    description='ETL pipeline with Spark and PostgreSQL',
    schedule_interval='*/10 8-23 * * *',  # Setiap 10 menit, mulai jam 8 sampai jam 23
    start_date=datetime(2024, 12, 20),
    catchup=False
)

# Path parameter untuk fleksibilitas
data_dir = '/home/hadoop/data_airflow'
spark_script_path = '/home/hadoop/airflow/scripts/transform_data.py'
jdbc_jar_path = '/home/hadoop/postgresql-42.2.26.jar'

# Task untuk mengecek data di database
check_data_evaluation = PostgresOperator(
    task_id='check_data_in_db_customer_transaction_history',
    postgres_conn_id='postgres_con',
    sql="""
        SELECT COUNT(*) FROM customer_transaction_history;
    """,
    do_xcom_push=True,
    dag=dag
)

check_bank_marketing = PostgresOperator(
    task_id='check_data_in_db_bank_marketing_dataset',
    postgres_conn_id='postgres_con',
    sql="SELECT COUNT(*) FROM bank_marketing_dataset;",
    do_xcom_push=True,
    dag=dag
)

check_credit_card_fraud = PostgresOperator(
    task_id='check_data_in_db_credit_card_fraud_dataset',
    postgres_conn_id='postgres_con',
    sql="SELECT COUNT(*) FROM credit_card_fraud_dataset;",
    do_xcom_push=True,
    dag=dag
)


check_loan_and_credit = PostgresOperator(
    task_id='check_data_in_db_loan_and_credit_dataset',
    postgres_conn_id='postgres_con',
    sql="SELECT COUNT(*) FROM loan_and_credit_dataset;",
    do_xcom_push=True,
    dag=dag
)


# Task Dummy untuk menangani kasus data tidak ditemukan
data_not_found_evaluation = EmptyOperator(
    task_id='data_not_found',
    dag=dag
)


# Task untuk menjalankan Spark menggunakan spark-submit
run_pyspark = BashOperator(
    task_id='spark',
    bash_command=f'spark-submit --jars {jdbc_jar_path} {spark_script_path}',
    dag=dag
)

# Fungsi branching untuk mengecek hasil dari keempat tabel
def check_data_branch(**context):
    ti = context['ti']
    results = {
        'customer_transaction': ti.xcom_pull(task_ids='check_data_in_db_customer_transaction_history'),
        'bank_marketing': ti.xcom_pull(task_ids='check_data_in_db_bank_marketing_dataset'),
        'credit_card_fraud': ti.xcom_pull(task_ids='check_data_in_db_credit_card_fraud_dataset'),
        'loan_and_credit': ti.xcom_pull(task_ids='check_data_in_db_loan_and_credit_dataset'),
    }
    
    logging.info(f"Results: {results}")
    
    # Cek apakah semua tabel memiliki data (COUNT > 0)
    if all(result and result[0][0] > 0 for result in results.values()):
        return 'spark'
    else:
        return 'data_not_found'


# Branching Operator
branching = BranchPythonOperator(
    task_id='branching',
    python_callable=check_data_branch,
    provide_context=True,
    dag=dag
)

def move_files():
    output_dir = "/home/hadoop/hasil_spark_tugas_akhir"
    final_dir = "/home/hadoop/data_transform_tugas_akhir"

    # Pastikan direktori tujuan ada
    if not os.path.exists(final_dir):
        os.makedirs(final_dir)

    # Daftar direktori hasil Spark yang perlu diproses
    datasets = [
        "transaction_summary",
        "evaluation_success_by_occupation",
        "fraudulent_transactions",
        "loan_data"
    ]

    for dataset in datasets:
        source_path = os.path.join(output_dir, dataset)
        dest_file = os.path.join(final_dir, f"{dataset}.csv")

        # Pastikan direktori sumber ada
        if os.path.exists(source_path):
            # Cari file part-*.csv di direktori hasil Spark
            part_files = [f for f in os.listdir(source_path) if f.startswith("part-") and f.endswith(".csv")]

            if part_files:
                # Menggabungkan semua part files ke dalam satu file tujuan
                with open(dest_file, 'wb') as outfile:
                    for part_file in part_files:
                        part_path = os.path.join(source_path, part_file)
                        with open(part_path, 'rb') as infile:
                            shutil.copyfileobj(infile, outfile)

# Mengecek apakah file CSV sudah ada dan tidak kosong
def validate_data():
    final_dir = "/home/hadoop/data_transform_tugas_akhir"
    datasets = [
        "transaction_summary",
        "evaluation_success_by_occupation",
        "fraudulent_transactions",
        "loan_data"
    ]

    for dataset in datasets:
        file_path = os.path.join(final_dir, f"{dataset}.csv")
        if os.path.exists(file_path) and os.path.getsize(file_path) > 0:
            logging.info(f"Validation passed for: {file_path}")
        else:
            raise ValueError(f"Validation failed for: {file_path}")

# Menyimpan log ke PostgreSQL
def log_to_postgres(status):
    hook = PostgresHook(postgres_conn_id='postgres_con')
    run_date = datetime.now()
    sql = f"""
        INSERT INTO airflow_pipeline_log (run_date, status)
        VALUES ('{run_date}', '{status}');
    """
    hook.run(sql)

# Task untuk log sukses
def log_success():
    log_to_postgres("SUCCESS")

# Task untuk log kegagalan
def log_failure():
    log_to_postgres("FAILURE")

# Task untuk memindahkan file hasil Spark
move_files_transform = PythonOperator(
    task_id='move_files_transform',
    python_callable=move_files,
    dag=dag
)

# Task untuk validasi data
validate_task = PythonOperator(
    task_id='validate_data',
    python_callable=validate_data,
    dag=dag
)

# Task untuk log sukses
log_success_task = PythonOperator(
    task_id='log_success',
    python_callable=log_success,
    dag=dag
)

# Task untuk log kegagalan
log_failure_task = PythonOperator(
    task_id='log_failure',
    python_callable=log_failure,
    trigger_rule='one_failed', 
    dag=dag
)

# Task untuk menghapus file sementara hasil Spark
clean_temp_files = BashOperator(
    task_id='clean_temp_files',
    bash_command='rm -rf /home/hadoop/data_transform_tugas_akhir/*',
    dag=dag
)

# Dependency antar task
# check_data_evaluation >> branching
# branching>>[run_pyspark, data_not_found_evaluation >> move_files_transform >> validate_task >> log_success_task]
# validate_task >> log_failure_task

# Dependency antar task
[check_data_evaluation, check_bank_marketing, check_credit_card_fraud, check_loan_and_credit] >> branching

# Cabang jika data ditemukan
branching >> run_pyspark
run_pyspark >> move_files_transform >> clean_temp_files
move_files_transform >> validate_task
validate_task >> [log_success_task, log_failure_task] 

# Cabang jika data tidak ditemukan
branching >> data_not_found_evaluation
data_not_found_evaluation >> log_failure_task