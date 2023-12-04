# DE_final_project
ETL process for tracking fraudulent transactions

The task is to develop an **ETL process** that receives a daily data upload
(provided in 3 days), uploads it to the data warehouse and **builds a report** daily.

# Data sources
1. PostgreSQL bank database that contains information about clients, accounts and cards of the bank
2. Transactions for one day in CSV files (1 file in 1 day)
3. List of ATMs in XLSX (1 file in 1 day, contains whole list)
4. List of banned passports XLSX (1 file in 1 day, contains monthly info)
   
Data comes from the files with the help of psycopg2 library. It updates according to the SCD2 approach.

# Expected outcome
Based on the download results, it is necessary to build a **fraudulent transactions report** on a daily basis. The showcase is built by accumulation,
each new report is placed in the same table with a new report_dt


