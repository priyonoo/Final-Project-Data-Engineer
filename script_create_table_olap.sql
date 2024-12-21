drop table evaluation_success_by_occupation;
drop table fraudulent_transactions;


CREATE TABLE loan_data (
    customer_id INT,
    loan_id VARCHAR(20),
    loan_amount DECIMAL(15, 2),
    interest_rate DECIMAL(5, 2),
    loan_term INT,
    loan_status VARCHAR(50),
    loan_date DATE,
    income DECIMAL(15, 2),
    transaction_id INT,
    transaction_amount DECIMAL(15, 2),
    transaction_date DATE,
    merchant VARCHAR(100)
);

CREATE TABLE fraudulent_transactions (
    transaction_id INT,
    transaction_date DATE,
    amount DECIMAL(15, 2),
    merchan_id INT,
    transaction_type VARCHAR(50),
    location VARCHAR(100),
    is_fraud BOOLEAN,
    customer_id INT,
    transaction_amount DECIMAL(15, 2),
    customer_transaction_date DATE,
    merchant VARCHAR(100),
    credit_card_number bigint
);

CREATE TABLE evaluation_success_by_occupation (
    occupation VARCHAR(100),
    count INT
);

truncate table fraudulent_transactions ;
truncate table evaluation_success_by_occupation ;
truncate table loan_data ;
truncate table transaction_summary ;
drop table fraudulent_transactions;
SELECT COUNT(*) AS total_rows
FROM fraudulent_transactions ft ;

SELECT COUNT(*) AS total_rows
FROM loan_data ld ;



