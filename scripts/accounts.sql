CREATE TABLE accounts (
    customer_id SERIAL PRIMARY KEY,
    first_name VARCHAR(100),
    last_name VARCHAR(100),
    address_1 VARCHAR(100),
    address_2 VARCHAR(100) NULL,
    city VARCHAR(100),
    state VARCHAR(100),
    zip_code INT,
    join_date DATE
);