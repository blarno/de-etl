CREATE TABLE zxcv_stg_terminals (
    terminal_id VARCHAR PRIMARY KEY,
    terminal_type VARCHAR,
    terminal_city VARCHAR,
    terminal_address VARCHAR
);

CREATE TABLE zxcv_stg_blacklist (
    passport_num VARCHAR PRIMARY KEY,
    entry_dt DATE
);

CREATE TABLE zxcv_stg_clients (
    client_id VARCHAR PRIMARY KEY,
    last_name VARCHAR,
    first_name VARCHAR,
    patrinymic VARCHAR,
    date_of_birth DATE,
    passport_num VARCHAR,
    passport_valid_to DATE,
    phone VARCHAR,
    create_dt timestamp,
	update_dt timestamp
);

CREATE TABLE zxcv_stg_accounts (
    account_num VARCHAR PRIMARY KEY,
    valid_to DATE,
    client VARCHAR,
    create_dt timestamp,
	update_dt timestamp
);

CREATE TABLE zxcv_stg_cards (
    card_num VARCHAR PRIMARY KEY,
    account_num VARCHAR,
    create_dt timestamp,
    update_dt timestamp
);

CREATE TABLE zxcv_stg_transactions (
    trans_id VARCHAR PRIMARY KEY,
    trans_date timestamp,
    card_num VARCHAR,
    oper_type VARCHAR,
    amt DECIMAL,
    oper_result VARCHAR,
    terminal VARCHAR
);


CREATE TABLE zxcv_dwh_dim_terminals (
    terminal_id VARCHAR PRIMARY KEY,
    terminal_type VARCHAR,
    terminal_city VARCHAR,
    terminal_address VARCHAR,
    create_dt timestamp,
    update_dt timestamp
);

CREATE TABLE zxcv_dwh_fact_passport_blacklist (
    passport_num VARCHAR PRIMARY KEY,
    entry_dt DATE
);

CREATE TABLE zxcv_dwh_dim_clients (
    client_id VARCHAR PRIMARY KEY,
    last_name VARCHAR,
    first_name VARCHAR,
    patrinymic VARCHAR,
    date_of_birth DATE,
    passport_num VARCHAR,
    passport_valid_to DATE,
    phone VARCHAR,
    create_dt timestamp,
    update_dt timestamp
);

CREATE TABLE zxcv_dwh_dim_accounts (
    account_num VARCHAR PRIMARY KEY,
    valid_to DATE,
    client VARCHAR,
    create_dt timestamp,
    update_dt timestamp,
    FOREIGN KEY (client) REFERENCES zxcv_dwh_dim_clients(client_id)
);

CREATE TABLE zxcv_dwh_dim_cards (
    card_num VARCHAR PRIMARY KEY,
    account_num VARCHAR,
    create_dt timestamp,
    update_dt timestamp,
    FOREIGN KEY (account_num) REFERENCES zxcv_dwh_dim_accounts(account_num)
);

CREATE TABLE zxcv_dwh_fact_transactions (
    trans_id VARCHAR PRIMARY KEY,
    trans_date timestamp,
    card_num VARCHAR,
    oper_type VARCHAR,
    amt DECIMAL,
    oper_result VARCHAR,
    terminal VARCHAR,
    FOREIGN KEY (card_num) REFERENCES zxcv_dwh_dim_cards(card_num),
    FOREIGN KEY (terminal) REFERENCES zxcv_dwh_dim_terminals(terminal_id)
);

CREATE TABLE zxcv_rep_fraud (
    trans_id VARCHAR,
    event_dt timestamp,
    passport VARCHAR,
    fio VARCHAR,
    phone VARCHAR,
    event_type VARCHAR,
    report_dt date
);