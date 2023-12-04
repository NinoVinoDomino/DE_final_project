---------------------------------|
-- drop if already exist tables  |
---------------------------------|

DROP TABLE IF EXISTS deaian.trsh_source_terminals;
DROP TABLE IF EXISTS deaian.trsh_source_cards;
DROP TABLE IF EXISTS deaian.trsh_source_accounts;
DROP TABLE IF EXISTS deaian.trsh_source_clients;
DROP TABLE IF EXISTS deaian.trsh_stg_passport_blacklist;
DROP TABLE IF EXISTS deaian.trsh_stg_transactions;
DROP TABLE IF EXISTS deaian.trsh_stg_terminals;
DROP TABLE IF EXISTS deaian.trsh_stg_terminals_del;
DROP TABLE IF EXISTS deaian.trsh_stg_cards;
DROP TABLE IF EXISTS deaian.trsh_stg_cards_del;
DROP TABLE IF EXISTS deaian.trsh_stg_accounts;
DROP TABLE IF EXISTS deaian.trsh_stg_accounts_del;
DROP TABLE IF EXISTS deaian.trsh_stg_clients;
DROP TABLE IF EXISTS deaian.trsh_stg_clients_del;
DROP TABLE IF EXISTS deaian.trsh_dwh_dim_clients;
DROP TABLE IF EXISTS deaian.trsh_dwh_dim_accounts;
DROP TABLE IF EXISTS deaian.trsh_dwh_dim_cards;
DROP TABLE IF EXISTS deaian.trsh_dwh_dim_terminals;
DROP TABLE IF EXISTS deaian.trsh_dwh_fact_passport_blacklist;
DROP TABLE IF EXISTS deaian.trsh_dwh_fact_transactions;
DROP TABLE IF EXISTS deaian.trsh_rep_fraud;

---------------------------|
-- source tables creation  |
---------------------------|

CREATE TABLE deaian.trsh_source_terminals(
    terminal_id varchar
    , terminal_type varchar
    , terminal_city varchar
    , terminal_address varchar
	, create_dt timestamp
	, update_dt timestamp
    , processed_dt timestamp(0) 
);

CREATE TABLE deaian.trsh_source_cards( 
card_num varchar
, account_num varchar
, create_dt timestamp
, update_dt timestamp
, processed_dt timestamp(0)
);

CREATE TABLE deaian.trsh_source_accounts (
account_num varchar
, valid_to timestamp(0)
, client varchar
, create_dt timestamp
, update_dt timestamp
, processed_dt timestamp(0)
);

CREATE TABLE deaian.trsh_source_clients( 
client_id varchar
, last_name varchar
, first_name varchar
, patronymic varchar
, date_of_birth timestamp(0)
, passport_num varchar
, passport_valid_to timestamp(0)
, phone varchar
, create_dt timestamp
, update_dt timestamp
, processed_dt timestamp(0) 
);

---------------------------|
-- staging tables creation |
---------------------------|


CREATE TABLE deaian.trsh_stg_transactions( 
    trans_id varchar
    , trans_date timestamp(0)
    , amt decimal(12, 3)
    , card_num varchar
    , oper_type varchar
    , oper_result varchar
    , terminal varchar
    , processed_dt timestamp(0) 
);

CREATE TABLE deaian.trsh_stg_terminals(
    terminal_id varchar
    , terminal_type varchar
    , terminal_city varchar
    , terminal_address varchar
	, create_dt timestamp
	, update_dt timestamp
    , processed_dt timestamp(0) 
);

CREATE TABLE deaian.trsh_stg_passport_blacklist( 
    passport_num varchar
    , entry_dt  timestamp(0)
	, create_dt timestamp
    , processed_dt timestamp(0) 
);

CREATE TABLE deaian.trsh_stg_cards( 
card_num varchar
, account_num varchar
, create_dt timestamp
, update_dt timestamp
, processed_dt timestamp(0)
);

CREATE TABLE deaian.trsh_stg_accounts (
account_num varchar
, valid_to timestamp(0)
, client varchar
, create_dt timestamp
, update_dt timestamp
, processed_dt timestamp(0)
);

CREATE TABLE deaian.trsh_stg_clients( 
client_id varchar
, last_name varchar
, first_name varchar
, patronymic varchar
, date_of_birth timestamp(0)
, passport_num varchar
, passport_valid_to timestamp(0)
, phone varchar
, create_dt timestamp
, update_dt timestamp
, processed_dt timestamp(0) 
);

---------------------------|
-- delete-tables creation  |
---------------------------|


CREATE TABLE deaian.trsh_stg_terminals_del( 
    terminal_id varchar 
    ,processed_dt timestamp(0)
);
CREATE TABLE deaian.trsh_stg_cards_del( 
    card_num varchar
    ,processed_dt timestamp(0)
);
CREATE TABLE deaian.trsh_stg_accounts_del( 
    account_num varchar 
    ,processed_dt timestamp(0)
);
CREATE TABLE deaian.trsh_stg_clients_del( 
    client_id varchar 
    ,processed_dt timestamp(0)
);



----------------------------|
-- metadata table creation  |
----------------------------|

CREATE TABLE deaian.trsh_metadata( 
    schema_name varchar(30)
    , table_name varchar(30)
    , max_update_dt timestamp(0)
);

-------------------------|
-- facts tables creation |
-------------------------|
CREATE TABLE deaian.trsh_dwh_fact_transactions( 
   trans_id varchar
    , trans_date timestamp
    , amt decimal(12, 3)
    , card_num varchar
    , oper_type varchar
    , oper_result varchar
    , terminal varchar
    , processed_dt timestamp(0)  
);

CREATE TABLE deaian.trsh_dwh_fact_passport_blacklist( 
    passport_num varchar
    , entry_dt timestamp
    , create_dt timestamp
    , processed_dt timestamp(0) 
);

-------------------------------|
-- dimensional tables creation |
-------------------------------|

CREATE TABLE deaian.trsh_dwh_dim_terminals( 
    terminal_id varchar
    , terminal_type varchar
    , terminal_city varchar
    , terminal_address varchar
    , create_dt  timestamp
    , update_dt timestamp
    , processed_dt timestamp(0) 
);

CREATE TABLE deaian.trsh_dwh_dim_cards( 
    card_num varchar
    , account_num varchar
    , create_dt timestamp
    , update_dt timestamp
    , processed_dt timestamp(0) 
);

CREATE TABLE deaian.trsh_dwh_dim_accounts( 
    account_num varchar
    , valid_to timestamp(0)
    , client varchar
    , create_dt timestamp
    , update_dt timestamp
    , processed_dt timestamp(0)
);

CREATE TABLE deaian.trsh_dwh_dim_clients( 
    client_id varchar
    , last_name varchar
    , first_name varchar
    , patronymic varchar
    , date_of_birth timestamp(0)
    , passport_num varchar
    , passport_valid_to timestamp(0)
    , phone varchar
    , create_dt timestamp
    , update_dt timestamp
    , processed_dt timestamp(0) 
);

CREATE TABLE deaian.trsh_rep_fraud( 
    event_dt timestamp
    , passport varchar
    , fio varchar
    , phone varchar
    , event_type varchar
    , report_dt timestamp(0)
);

-------------------------------|
-- metadata table data insert  |
-------------------------------|

INSERT INTO deaian.trsh_metadata( schema_name, table_name, max_update_dt ) 
VALUES( 'deaian'
     ,'trsh_source_terminals'
     , to_date('1800-01-01','YYYY-MM-DD')
     );
INSERT INTO deaian.trsh_metadata( schema_name, table_name, max_update_dt ) 
VALUES( 'deaian'
     ,'trsh_source_cards'
     , to_date('1800-01-01','YYYY-MM-DD')
     );
INSERT INTO deaian.trsh_metadata( schema_name, table_name, max_update_dt ) 
VALUES( 'deaian'
    ,'trsh_source_accounts'
    , to_date('1800-01-01','YYYY-MM-DD')
    );
INSERT INTO deaian.trsh_metadata( schema_name, table_name, max_update_dt ) 
VALUES( 'deaian'
    ,'trsh_source_clients'
    , to_date('1800-01-01','YYYY-MM-DD')
    );
INSERT INTO deaian.trsh_metadata( schema_name, table_name, max_update_dt ) 
VALUES( 'deaian'
    ,'trsh_rep_fraud'
    , to_date('1800-01-01','YYYY-MM-DD')
    );
INSERT INTO deaian.trsh_metadata( schema_name, table_name, max_update_dt ) 
VALUES( 'deaian'
    ,'trsh_dwh_fact_transactions'
    , to_date('1800-01-01','YYYY-MM-DD')
    );	
	
	
