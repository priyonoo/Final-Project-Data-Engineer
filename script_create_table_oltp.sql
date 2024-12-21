create table bank_marketing_dataset(
	id SERIAL PRIMARY KEY,
    occupation VARCHAR(50),
    age INT,
    education_level VARCHAR(50),
    marital_status VARCHAR(20),
    communication_channel VARCHAR(20),
    call_month VARCHAR(20),
    call_day INT,
    call_duration INT,
    call_frequency INT,
    previous_campaign_outcome VARCHAR(20),
    conversion_status VARCHAR(20)
);

drop table bank_marketing_dataset ;

create table loan_and_credit_dataset(
	loan_id VARCHAR(10) PRIMARY KEY,
    customer_id INT,
    loan_amount DECIMAL(10, 2),
    interest_rate DECIMAL(5, 2),
    loan_term INT,
    loan_status VARCHAR(20),
    loan_date DATE,
    income DECIMAL(10, 2)
);

create table credit_card_fraud_dataset(
	transaction_id INT PRIMARY KEY,
    transaction_date DATE,
    amount DECIMAL(10, 2),
    merchan_id INT,
    transaction_type VARCHAR(20),
    location VARCHAR(50),
    is_fraud int,
    credit_card_number bigint
);

drop table credit_card_fraud_dataset;

create table customer_transaction_history(
	transaction_id INT,
    customer_id INT,
    transaction_amount INT,
    transaction_date date,
    merchant VARCHAR(100)
);

drop table credit_card_fraud_dataset  ;

CREATE TABLE airflow_pipeline_log (
    id SERIAL PRIMARY KEY,
    run_date TIMESTAMP,
    status VARCHAR(50)
);
