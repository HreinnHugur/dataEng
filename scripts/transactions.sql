CREATE TABLE transactions (
    transaction_id VARCHAR(64) PRIMARY KEY,
    transaction_date DATE,
    product_id INT,
    product_code INT,
    product_description VARCHAR(64),
    quantity INT,
    account_id INT,

    CONSTRAINT product_transaction_constraint
        FOREIGN KEY (product_id) 
        REFERENCES products(product_id),
    CONSTRAINT account_transaction_constraint
        FOREIGN KEY (account_id) 
        REFERENCES accounts(customer_id)
);