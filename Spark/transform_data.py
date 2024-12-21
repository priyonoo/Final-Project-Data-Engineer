from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType
from pyspark.sql.functions import col, udf
import os

def init_spark():
    return SparkSession.builder \
        .appName("TransformData") \
        .getOrCreate()

# Membaca data dari database PostgreSQL menggunakan JDBC
def read_from_postgres(spark, jdbc_url, table_name, properties):
    return spark.read.jdbc(url=jdbc_url, table=table_name, properties=properties)

# Menulis DataFrame ke database PostgreSQL menggunakan JDBC
def write_to_postgres(df, jdbc_url, table_name, properties):
    df.write.jdbc(
        url=jdbc_url,
        table=table_name,
        mode="overwrite",
        properties=properties
    )

def analyze_transactions(customer_df):
    return customer_df.groupBy("customer_id") \
        .agg(
            F.count("transaction_id").alias("total_transactions"),
            F.sum("transaction_amount").alias("total_amount")
        ).orderBy(F.col("total_transactions").desc())

def evaluate_marketing(marketing_df):
    successful_campaigns = marketing_df.filter(marketing_df['conversion_status'] == 'converted')
    return successful_campaigns.groupBy("occupation").count().orderBy(F.col("count").desc())

# Fungsi masking credit_card_number
def mask_credit_card(card_number):
    card_str = str(card_number)
    if len(card_str) >= 8:
        return card_str[:2] + '*' * (len(card_str) - 8) + card_str[-2:]
    return card_str

# Fungsi masking location
def mask_location(location):
    if not location:
        return location
    words = location.split()
    if len(words) >= 2:
        return words[0] + ' ' + '*' * len(words[1])
    elif len(words) == 1:
        return words[0][:3] + '*' * (len(words[0]) - 3)
    return location

# Register UDFs untuk masking
mask_credit_card_udf = udf(mask_credit_card, StringType())
mask_location_udf = udf(mask_location, StringType())

def detect_fraud(credit_df, customer_df):
    fraud_transactions = credit_df.join(
        customer_df.withColumnRenamed("transaction_date", "customer_transaction_date"),
        "transaction_id",
        "inner"
    ).filter(col('is_fraud') == 1)

    # Masking kolom credit_card_number dan location
    masked_fraud = fraud_transactions \
        .withColumn("credit_card_number", mask_credit_card_udf(col("credit_card_number"))) \
        .withColumn("location", mask_location_udf(col("location")))
    
    return masked_fraud

def process_loan_data(loan_df, customer_df):
    return loan_df.join(customer_df, "customer_id", "inner")

def write_to_local(df, path, format="csv", mode="overwrite", header=True):
    if format == "csv":
        df.write.csv(path, header=header, mode=mode)
    elif format == "parquet":
        df.write.parquet(path, mode=mode)
    elif format == "json":
        df.write.json(path, mode=mode)
    else:
        raise ValueError(f"Format {format} tidak didukung.")

if __name__ == "__main__":
    spark = init_spark()

    # Konfigurasi JDBC untuk membaca dari database OLTP
    read_jdbc_url = "jdbc:postgresql://172.27.42.224:5432/tugas_akhir"
    read_jdbc_properties = {
        "user": "postgres",
        "password": "agipriyono",
        "driver": "org.postgresql.Driver"
    }

    # Konfigurasi JDBC untuk menulis ke database data_olap
    write_jdbc_url = "jdbc:postgresql://172.27.42.224:5432/data_olap"
    write_jdbc_properties = {
        "user": "postgres",
        "password": "agipriyono",
        "driver": "org.postgresql.Driver"
    }

    # Analisis transaksi
    customer_df = read_from_postgres(spark, read_jdbc_url, "customer_transaction_history", read_jdbc_properties)
    transaction_summary = analyze_transactions(customer_df)
    write_to_postgres(transaction_summary, write_jdbc_url, "transaction_summary", write_jdbc_properties)
    write_to_local(transaction_summary, "/home/hadoop/hasil_spark_tugas_akhir/transaction_summary", format="csv")

    # Evaluasi kampanye pemasaran
    marketing_df = read_from_postgres(spark, read_jdbc_url, "bank_marketing_dataset", read_jdbc_properties)
    success_by_occupation = evaluate_marketing(marketing_df)
    write_to_postgres(success_by_occupation, write_jdbc_url, "evaluation_success_by_occupation", write_jdbc_properties)
    write_to_local(success_by_occupation, "/home/hadoop/hasil_spark_tugas_akhir/evaluation_success_by_occupation", format="csv")
    
    # Deteksi penipuan dengan masking
    credit_df = read_from_postgres(spark, read_jdbc_url, "credit_card_fraud_dataset", read_jdbc_properties)
    fraud_result = detect_fraud(credit_df, customer_df)
    write_to_postgres(fraud_result, write_jdbc_url, "fraudulent_transactions", write_jdbc_properties)
    write_to_local(fraud_result, "/home/hadoop/hasil_spark_tugas_akhir/fraudulent_transactions", format="csv")

    # Data pinjaman dan transaksi
    loan_df = read_from_postgres(spark, read_jdbc_url, "loan_and_credit_dataset", read_jdbc_properties)
    combined_loan_data = process_loan_data(loan_df, customer_df)
    write_to_postgres(combined_loan_data, write_jdbc_url, "loan_data", write_jdbc_properties)
    write_to_local(combined_loan_data, "/home/hadoop/hasil_spark_tugas_akhir/loan_data", format="csv")

    spark.stop()
