#!/usr/bin/python3


import psycopg2
import pandas as pd
import re
import os
import datetime

###########################################
#Шаг 0. подключение к базам данных
###########################################

# Создание подключения к edu
conn1 = psycopg2.connect(database = "edu", 
                         host ="de-edu-db.chronosavant.ru", 
                         user =  "deaian", 
                         password = "sarumanthewhite", 
                         port ="5432")

# Создание подключения к bank.
conn2 = psycopg2.connect(database = "bank", 
                         host ="de-edu-db.chronosavant.ru", 
                         user =  "bank_etl", 
                         password = "bank_etl_password", 
                         port ="5432")

# Отключение автокоммита
conn1.autocommit = False
conn2.autocommit = False

# Создание курсора
cursor1 = conn1.cursor() #edu
cursor2 = conn2.cursor() #bank

###########################################
#Шаг 1. Очищаем весь стейджинг
###########################################


cursor1.execute("delete from deaian.trsh_stg_terminals_del")
cursor1.execute("delete from deaian.trsh_stg_cards_del")
cursor1.execute("delete from deaian.trsh_stg_accounts_del")
cursor1.execute("delete from deaian.trsh_stg_clients_del")

cursor1.execute("delete from deaian.trsh_stg_passport_blacklist")
cursor1.execute("delete from deaian.trsh_stg_transactions")

cursor1.execute( "delete from deaian.trsh_stg_terminals")
cursor1.execute( "delete from deaian.trsh_stg_terminals_del")
cursor1.execute( "delete from deaian.trsh_stg_clients")
cursor1.execute( "delete from deaian.trsh_stg_clients_del")
cursor1.execute( "delete from deaian.trsh_stg_accounts")
cursor1.execute( "delete from deaian.trsh_stg_accounts_del")
cursor1.execute( "delete from deaian.trsh_stg_cards")
cursor1.execute( "delete from deaian.trsh_stg_cards_del")




###########################################
#Шаг 2. Наполнение стейджинг таблиц данными
###########################################


#2.1 при помощи модуля os будем искать нужные для загрузки файлы
path = "/home/deaian/trsh/project"
files = os.listdir(path)
name_files_transactions= re.findall('tr\w+.txt', " ".join(files))
name_files_terminals = re.findall('ter\w+.xlsx', " ".join(files))
name_files_blacklists = re.findall('pas\w+.xlsx', " ".join(files))
data= re.findall('\d+', " ".join(files))
data_load = datetime.datetime.strptime(data[0], '%d%m%Y').date()

#кладем данные из файлов в датафрейм
try:
    df_transactions = pd.read_csv(path + '/' + name_files_transactions[0], sep=';', decimal=',')
    df_terminals = pd.read_excel(path + '/' + name_files_terminals[0], sheet_name='terminals', header=0, index_col=None )
    df_terminals['data_load'] = data_load
    df_blacklists = pd.read_excel(path + '/' + name_files_blacklists[0], sheet_name='blacklist', header=0, index_col=None )
except FileNotFoundError as f:
	print(f)
    
#и перекладываем их в стейджинговые таблицы (базы edu)
cursor1.executemany( "INSERT INTO deaian.trsh_stg_transactions( trans_id, trans_date, amt, card_num, oper_type, oper_result, terminal ) VALUES ( %s, %s ,%s, %s,%s,%s,%s )", df_transactions.values.tolist() )
cursor1.executemany( "INSERT INTO deaian.trsh_source_terminals( terminal_id, terminal_type, terminal_city, terminal_address, update_dt ) VALUES ( %s,%s,%s,%s, %s)", df_terminals.values.tolist() )
cursor1.executemany( "INSERT INTO deaian.trsh_stg_passport_blacklist( entry_dt, passport_num ) VALUES ( %s,%s )", df_blacklists.values.tolist() )

conn1.commit()
cursor2.execute("SELECT card_num, account, create_dt, update_dt FROM bank.info.cards")
cards = cursor2.fetchall()
names_cards = [ x[0] for x in cursor2.description ]
df_cards = pd.DataFrame( cards, columns = names_cards )
# Accounts
cursor2.execute("SELECT account, valid_to, client, create_dt, update_dt FROM bank.info.accounts")
accounts = cursor2.fetchall()
names_accounts = [ x[0] for x in cursor2.description ]
df_accounts = pd.DataFrame( accounts, columns = names_accounts )
# Clients
cursor2.execute("SELECT client_id, last_name, first_name, patronymic, date_of_birth, passport_num, passport_valid_to, phone, create_dt, update_dt  FROM bank.info.clients")
clients = cursor2.fetchall()
names_clients = [ x[0] for x in cursor2.description ]
df_clients = pd.DataFrame( clients, columns = names_clients )

# 1.3. Наполнение stg-слоя в базе EDU
cursor1.executemany( "INSERT INTO deaian.trsh_source_cards( card_num, account_num, create_dt, update_dt ) VALUES ( %s,%s,%s,%s)", df_cards.values.tolist() )
cursor1.executemany( "INSERT INTO deaian.trsh_source_accounts( account_num, valid_to, client, create_dt, update_dt ) VALUES ( %s,%s,%s,%s,%s )", df_accounts.values.tolist() )
cursor1.executemany( """
		INSERT INTO deaian.trsh_source_clients(
                            client_id,
                            last_name,
                            first_name,
                            patronymic,
                            date_of_birth,
                            passport_num,
                            passport_valid_to,
                            phone,
                            create_dt,
                            update_dt ) VALUES ( %s,%s,%s,%s,%s,%s,%s,%s,%s,%s )""", df_clients.values.tolist())
# Закрываем соединение bank
cursor2.close()
conn2.close()

#отработанные файлы перемещаем в архив
transactions_to_archive = path + '/archive/' + name_files_transactions[0] + '.backup'
terminals_to_archive = path + '/archive/' + name_files_terminals[0] + '.backup'
blacklists_to_archive = path + '/archive/' + name_files_blacklists[0] + '.backup'

try:
	os.rename(path + '/' + name_files_transactions[0], transactions_to_archive)
	print("Transactions moved to archive")
	os.rename(path + '/' + name_files_terminals[0], terminals_to_archive)
	print("Terminals moved to archive")
	os.rename(path + '/' + name_files_blacklists[0], blacklists_to_archive)
	print("Blacklists moved to archive")
except OSError as error:
	print(error)
#########################################################################
#Шаг 3. Загрузка данных из стейджинга, удаления и управления метаданными. 
#########################################################################


#SCD1 для TRSH_STG_TERMINALS

#Шаг 1. Стейджинг очищен выше
#Шаг 2. захват данных с источника-- в processed_dt всегда ложится now() [в create_dt легло update_dt из stg таблицы]
cursor1.execute( """
    insert into deaian.trsh_stg_terminals ( terminal_id, terminal_type, terminal_city, terminal_address, create_dt, update_dt, processed_dt)
    select terminal_id, terminal_type, terminal_city, terminal_address, create_dt, now(), now() from deaian.trsh_source_terminals
    where update_dt > (
       select max_update_dt
       from deaian.trsh_metadata
       where schema_name = 'deaian' and table_name = 'trsh_source_terminals'
    )
    """
)

cursor1.execute( """
    insert into deaian.trsh_stg_terminals_del (terminal_id )
    select terminal_id  from deaian.trsh_source_terminals
    """
)


#Шаг3. Применение данных (накатка) в приемник DDS (вставка)
cursor1.execute( """
    insert into deaian.trsh_dwh_dim_terminals ( terminal_id, terminal_type, terminal_city, terminal_address, create_dt, update_dt, processed_dt )
    select 
    	stg.terminal_id,
    	stg.terminal_type,
    	stg.terminal_city,
    	stg.terminal_address,
    	stg.update_dt, 
    	null,
    	now()
    from deaian.trsh_stg_terminals stg
    left join deaian.trsh_dwh_dim_terminals tgt
    on stg.terminal_id = tgt.terminal_id
    where tgt.terminal_id is null  
    """
)

#Шаг4. Применение данных в приемник DDS (обновление)
cursor1.execute( """
    update deaian.trsh_dwh_dim_terminals 
    set 
       terminal_type = tmp.terminal_type,
       terminal_city = tmp.terminal_city,
       terminal_address = tmp.terminal_address,
       update_dt = tmp.update_dt,
       processed_dt = now()
    from (
     select stg.terminal_id,
    	stg.terminal_type,
    	stg.terminal_city,
    	stg.terminal_address,
    	stg.update_dt
    
     from deaian.trsh_stg_terminals stg
     inner join deaian.trsh_dwh_dim_terminals tgt
     on stg.terminal_id = tgt.terminal_id
     where stg.terminal_type <> tgt.terminal_type 
	     or (stg.terminal_type is null and tgt.terminal_type is not null) 
	     or (stg.terminal_type is not null and tgt.terminal_type is null) 
	 ) tmp
    where trsh_dwh_dim_terminals.terminal_id = tmp.terminal_id
    """
) 

#Шаг5. Применение данных в приемник DDS (удаление)

cursor1.execute( """
    delete from deaian.trsh_dwh_dim_terminals
    where terminal_id in (
      select 
           tgt.terminal_id
      from deaian.trsh_dwh_dim_terminals tgt
      left join deaian.trsh_stg_terminals_del stg
      on tgt.terminal_id = stg.terminal_id
      where stg.terminal_id is null
    )
    """
)

#Шаг6. Сохраняем состояние загрузки в метаданные 
cursor1.execute( """
    update deaian.trsh_metadata
    set max_update_dt = (select max( coalesce( update_dt, create_dt ))
    from deaian.trsh_stg_terminals)
    where schema_name = 'deaian' and table_name = 'trsh_source_terminals'
    """
)

#Шаг7. Фиксация транзакции

conn1.commit()



#SCD1 для TRSH_STG_CARDS

#Шаг 1. Стейджинг очищен выше
#Шаг 2. захват данных с источника-- в processed_dt всегда ложится now() [в create_dt легло update_dt из stg таблицы]

cursor1.execute( """
    insert into deaian.trsh_stg_cards ( card_num, account_num, create_dt, update_dt, processed_dt)
    select card_num, account_num, update_dt, create_dt,  now() from deaian.trsh_source_cards
    where create_dt > (
       select max_update_dt
       from deaian.trsh_metadata
       where schema_name = 'deaian' and table_name = 'trsh_source_cards'
    )
    """
)
cursor1.execute( """
    insert into deaian.trsh_stg_cards_del ( card_num)
    select card_num from deaian.trsh_source_cards
    """
)
#Шаг3. Применение данных (накатка) в приемник DDS (вставка)
cursor1.execute( """
    insert into deaian.trsh_dwh_dim_cards ( card_num, account_num, create_dt, update_dt, processed_dt )
    select 
    	stg.card_num,
    	stg.account_num,
    	stg.update_dt, 
    	null,
    	now()
    from deaian.trsh_stg_cards stg
    left join deaian.trsh_dwh_dim_cards tgt
    on stg.card_num = tgt.card_num
    where tgt.card_num is null
    """
)


#Шаг4. Применение данных в приемник DDS (обновление)
cursor1.execute( """
    update deaian.trsh_dwh_dim_cards 
    set 
       card_num = tmp.card_num,
       account_num = tmp.account_num,
       update_dt = tmp.update_dt,
       processed_dt = now()
    from (
     select stg.card_num,
    	stg.account_num,
    	stg.update_dt
    
     from deaian.trsh_stg_cards stg
     inner join deaian.trsh_dwh_dim_cards tgt
     on stg.card_num = tgt.card_num
     where stg.account_num <> tgt.account_num 
	     or (stg.account_num is null and tgt.account_num is not null) 
	     or (stg.account_num is not null and tgt.account_num is null) 
	 ) tmp
    where trsh_dwh_dim_cards.card_num = tmp.card_num
    """
)


#Шаг5. Применение данных в приемник DDS (удаление)

cursor1.execute( """
    delete from deaian.trsh_dwh_dim_cards
    where card_num in (
      select 
           tgt.card_num
      from deaian.trsh_dwh_dim_cards tgt
      left join deaian.trsh_stg_cards_del stg
      on tgt.card_num = stg.card_num
      where stg.card_num is null
    )
    """
)

#Шаг6. Сохраняем состояние загрузки в метаданные 
cursor1.execute( """
    update deaian.trsh_metadata
    set max_update_dt = (select max( coalesce( update_dt, create_dt ))
    from deaian.trsh_stg_cards)
    where schema_name = 'deaian' and table_name = 'trsh_source_cards'
    """
)

#Шаг7. Фиксация транзакции

conn1.commit()

#SCD1 для TRSH_STG_ACCOUNTS 

#Шаг 1. Стейджинг очищен выше
#Шаг 2. захват данных с источника-- в processed_dt всегда ложится now() [в create_dt легло update_dt из stg таблицы]
cursor1.execute( """
    insert into deaian.trsh_stg_accounts ( account_num, valid_to, client, create_dt, update_dt, processed_dt)
    select account_num, valid_to, client, update_dt, create_dt, now() from deaian.trsh_source_accounts  
    where create_dt > (
       select max_update_dt
       from deaian.trsh_metadata 
       where schema_name = 'deaian' and table_name = 'trsh_source_accounts'
    )
    """
)
cursor1.execute( """
    insert into deaian.trsh_stg_accounts_del ( account_num)
    select account_num from deaian.trsh_source_accounts 
    """
)
#Шаг3. Применение данных (накатка) в приемник DDS (вставка)
cursor1.execute( """
    insert into deaian.trsh_dwh_dim_accounts ( account_num, valid_to, client, create_dt, update_dt, processed_dt )
    select 
    	stg.account_num,
    	stg.valid_to,
    	stg.client,
    	stg.update_dt, 
    	null,
    	now()
    from deaian.trsh_stg_accounts  stg
    left join deaian.trsh_dwh_dim_accounts  tgt
    on stg.account_num = tgt.account_num
    where tgt.account_num is null
    """
)


#Шаг4. Применение данных в приемник DDS (обновление)
cursor1.execute( """
    update deaian.trsh_dwh_dim_accounts 
    set 
       valid_to = tmp.valid_to,
       client = tmp.client,
       update_dt = tmp.update_dt,
       processed_dt = now()
    from (
     select stg.account_num,
    	stg.valid_to,
    	stg.client,
    	stg.update_dt
    
     from deaian.trsh_stg_accounts stg
     inner join deaian.trsh_dwh_dim_accounts tgt
     on stg.account_num = tgt.account_num
     where stg.client <> tgt.client
	     or (stg.client is null and tgt.client is not null) 
	     or (stg.client is not null and tgt.client is null) 
	 ) tmp
    where trsh_dwh_dim_accounts.account_num = tmp.account_num
    """
) 

#Шаг5. Применение данных в приемник DDS (удаление)

cursor1.execute( """
    delete from deaian.trsh_dwh_dim_accounts
    where account_num in (
      select 
           tgt.account_num
      from deaian.trsh_dwh_dim_accounts tgt
      left join deaian.trsh_stg_accounts_del stg
      on tgt.account_num= stg.account_num
      where stg.account_num is null
    )
    """
)

#Шаг6. Сохраняем состояние загрузки в метаданные 
cursor1.execute( """
    update deaian.trsh_metadata
    set max_update_dt = (select max( coalesce( update_dt, create_dt ))
    from deaian.trsh_stg_accounts)
    where schema_name = 'deaian' and table_name = 'trsh_source_accounts'
   """
 )

#Шаг7. Фиксация транзакции

conn1.commit()

#SCD1 для TRSH_STG_CLIENTS

#Шаг 1. Стейджинг очищен выше
#Шаг 2. захват данных с источника-- в processed_dt всегда ложится now() [в create_dt легло update_dt из stg таблицы]
cursor1.execute( """
    insert into deaian.trsh_stg_clients ( client_id, last_name, first_name, patronymic, date_of_birth, passport_num, passport_valid_to, phone, create_dt, update_dt, processed_dt)
    select client_id, last_name, first_name, patronymic, date_of_birth, passport_num, passport_valid_to, phone, update_dt, create_dt, now() from deaian.trsh_source_clients
    where create_dt > (
       select max_update_dt
       from deaian.trsh_metadata
       where schema_name = 'deaian' and table_name = 'trsh_source_clients'
    )
    """
)
cursor1.execute( """
    insert into deaian.trsh_stg_clients_del ( client_id)
    select client_id from deaian.trsh_source_clients
    """
)
#Шаг3. Применение данных (накатка) в приемник DDS (вставка)
cursor1.execute( """
    insert into deaian.trsh_dwh_dim_clients ( client_id, last_name, first_name, patronymic, date_of_birth, passport_num, passport_valid_to, phone, create_dt, update_dt, processed_dt )
    select 
    	stg.client_id,
    	stg.last_name,
    	stg.first_name,
		stg.patronymic,
		stg.date_of_birth,
		stg.passport_num,
		stg.passport_valid_to,
		stg.phone,
    	stg.update_dt, 
    	null,
    	now()
    from deaian.trsh_stg_clients  stg
    left join deaian.trsh_dwh_dim_clients tgt
    on stg.client_id = tgt.client_id
    where tgt.client_id is null
    """
)

#Шаг4. Применение данных в приемник DDS (обновление)
cursor1.execute( """
    update deaian.trsh_dwh_dim_clients 
    set 
	   last_name = tmp.last_name,
       first_name = tmp.first_name,
	   patronymic = tmp.patronymic,
	   date_of_birth = tmp.date_of_birth,
	   passport_num = tmp.passport_num,
	   passport_valid_to = tmp.passport_valid_to,
	   phone = tmp.phone,
       update_dt = tmp.update_dt,
       processed_dt = now()
    from (
     select stg.client_id,
    	stg.last_name,
    	stg.first_name,
		stg.patronymic,
		stg.date_of_birth,
		stg.passport_num,
		stg.passport_valid_to,
		stg.phone,
    	stg.update_dt
    
     from deaian.trsh_stg_clients stg
     inner join deaian.trsh_dwh_dim_clients tgt
     on stg.client_id = tgt.client_id
     where stg.passport_num  <> tgt.passport_num 
	     or (stg.passport_num  is null and tgt.passport_num  is not null) 
	     or (stg.passport_num  is not null and tgt.passport_num  is null) 
	 ) tmp
    where trsh_dwh_dim_clients.client_id = tmp.client_id
    """
)


#Шаг5. Применение данных в приемник DDS (удаление)

cursor1.execute( """
    delete from deaian.trsh_dwh_dim_clients 
    where client_id in (
      select 
           tgt.client_id
      from deaian.trsh_dwh_dim_clients tgt
      left join deaian.trsh_stg_clients_del stg
      on tgt.client_id= stg.client_id
      where stg.client_id is null
    )
    """
)

#Шаг6. Сохраняем состояние загрузки в метаданные 
cursor1.execute( """
    update deaian.trsh_metadata 
    set max_update_dt = (select max( coalesce( update_dt, create_dt ))
    from deaian.trsh_stg_clients )
    where schema_name = 'deaian' and table_name = 'trsh_source_clients'
  """
 )

#Шаг7. Фиксация транзакции

conn1.commit()

#Загрузка фактовых таблиц trsh_dwh_fact_passport_blacklist и trsh_dwh_fact_transactions

cursor1.execute( """
    insert into deaian.trsh_dwh_fact_transactions( trans_id, trans_date, card_num, oper_type, amt, oper_result, terminal, processed_dt )
    select trans_id, trans_date, card_num, oper_type, amt, oper_result, terminal, now() from deaian.trsh_stg_transactions
	"""
)

cursor1.execute( """
    insert into deaian.trsh_dwh_fact_passport_blacklist( passport_num, entry_dt, processed_dt )
    select passport_num, entry_dt, now() from deaian.trsh_stg_passport_blacklist
   """
)

#########################################################################
#ПОСТРОЕНИЕ ОТЧЕТА
#########################################################################

#Для певрого типа мошеничества
# Совершение операций при просроченном или заблокированном паспорте 
cursor1.execute("""
                INSERT INTO deaian.trsh_rep_fraud ( event_dt, passport, fio, phone, event_type, report_dt)
                SELECT 
                    trans_date event_dt
                    ,passport_num passport
                    ,last_name || ' ' || first_name || ' ' || patronymic fio
                    ,phone
                    ,'1' event_type
                    ,trans_date::date report_dt
                FROM (
                    SELECT
                        trans_date
                        ,cl.passport_num
                        ,last_name
                        ,first_name
                        ,patronymic
                        ,phone
                    FROM deaian.trsh_dwh_fact_transactions tr
                    LEFT JOIN deaian.trsh_dwh_dim_cards c on trim(tr.card_num) = trim(c.card_num)
                    LEFT JOIN deaian.trsh_dwh_dim_accounts ac ON c.account_num = ac.account_num
                    LEFT JOIN deaian.trsh_dwh_dim_clients cl ON ac.client = cl.client_id
                    WHERE passport_num IN 
                           (SELECT passport_num FROM deaian.trsh_dwh_fact_passport_blacklist) 
                            OR passport_valid_to < (select distinct(trans_date::date) FROM deaian.trsh_stg_transactions)
                    ) as tmp	
                     WHERE trans_date > (select
					max_update_dt
                                FROM deaian.trsh_metadata
				    WHERE schema_name = 'deaian'
                                AND table_name = 'trsh_dwh_fact_transactions'
                                    )
                """)


#Для второго типа мошеничества
#Совершение операций при недействующем договоре

cursor1.execute("""
            INSERT INTO deaian.trsh_rep_fraud ( event_dt, passport, fio, phone, event_type, report_dt)
                SELECT 
                    trans_date event_dt
                    ,passport_num passport
                    ,last_name || ' ' || first_name || ' ' || patronymic fio
                    ,phone
                    ,'2' event_type
                    ,trans_date report_dt
                FROM (
                    SELECT
                        trans_date
                        ,cl.passport_num
                        ,last_name
                        ,first_name
                        ,patronymic
                        ,phone
                    FROM deaian.trsh_dwh_fact_transactions tr
                    LEFT JOIN deaian.trsh_dwh_dim_cards c on trim(tr.card_num) = trim(c.card_num)
                    LEFT JOIN deaian.trsh_dwh_dim_accounts ac ON c.account_num = ac.account_num
                    LEFT JOIN deaian.trsh_dwh_dim_clients cl ON ac.client = cl.client_id
                    WHERE valid_to < (select distinct(trans_date::date) FROM deaian.trsh_stg_transactions)
                 ) tmp
                 
                 WHERE trans_date > (select max_update_dt
                                    FROM deaian.trsh_metadata
				                    WHERE schema_name = 'deaian'
                                    AND table_name = 'trsh_dwh_fact_transactions'
                                     )
                                     
                                     
            """)                         


#Для третьего вида мошеничества
#Совершение операций в разных городах в течении одного часа

cursor1.execute("""
      INSERT INTO deaian.trsh_rep_fraud ( event_dt, passport, fio, phone, event_type, report_dt)
                SELECT
                    trans_date event_dt
                    ,passport_num passport
                    ,last_name || ' ' || first_name || ' ' || patronymic fio
                    ,phone
                    ,'3' event_type
                    ,trans_date::date report_dt
                FROM (
                    SELECT
                        trans_date
                        ,cl.passport_num
                        ,last_name
                        ,first_name
                        ,patronymic
                        ,phone
                        ,DENSE_RANK() OVER(PARTITION BY EXTRACT (HOUR FROM trans_date), tr.card_num ORDER BY terminal_city) city_rnk
                    FROM deaian.trsh_dwh_fact_transactions tr
                    LEFT JOIN deaian.trsh_dwh_dim_cards c on trim(tr.card_num) = trim(c.card_num)
                    LEFT JOIN deaian.trsh_dwh_dim_accounts ac ON c.account_num = ac.account_num
                    LEFT JOIN deaian.trsh_dwh_dim_clients cl ON ac.client = cl.client_id
                    LEFT JOIN deaian.trsh_dwh_dim_terminals ter ON tr.terminal = ter.terminal_id
                    ORDER BY trans_date) a
                WHERE city_rnk > 1
                AND trans_date > (
                                select
					max_update_dt
                                FROM deaian.trsh_metadata
                                WHERE schema_name = 'deaian'
                                AND table_name = 'trsh_dwh_fact_transactions'
                                    )
                                    
                                    
              """) 
              
  
#Для четвертого вида мошеничества
#несколько попыток подбора суммы с успехом в последней операции
cursor1.execute("""
   INSERT INTO deaian.trsh_rep_fraud ( event_dt, passport, fio, phone, event_type, report_dt)
                SELECT 
                    trans_date event_dt
                    ,passport_num passport
                    ,last_name || ' ' || first_name || ' ' || patronymic fio
                    ,phone
                    ,'4' event_type
                    ,trans_date::date report_dt
                FROM (
                    SELECT 
                        trans_date
                        ,cl.passport_num
                        ,oper_result
                        ,amt
                        ,last_name
                        ,first_name
                        ,patronymic
                        ,phone
                        ,LAG (trans_date, 3) over(PARTITION BY tr.card_num ORDER BY trans_date) previous_time
                        ,LAG (amt, 3) over(PARTITION BY tr.card_num ORDER BY trans_date) amt_3
                        ,LAG (amt, 2) over(PARTITION BY tr.card_num ORDER BY trans_date) amt_2
                        ,LAG (amt, 1) over(PARTITION BY tr.card_num ORDER BY trans_date) amt_1
                        ,LAG (oper_result, 3) over(PARTITION BY tr.card_num ORDER BY trans_date) res_3
                        ,LAG (oper_result, 2) over(PARTITION BY tr.card_num ORDER BY trans_date) res_2
                        ,LAG (oper_result, 1) over(PARTITION BY tr.card_num ORDER BY trans_date) res_1
                    FROM deaian.trsh_dwh_fact_transactions tr
                    LEFT JOIN deaian.trsh_dwh_dim_cards c on trim(tr.card_num) = trim(c.card_num)
                    LEFT JOIN deaian.trsh_dwh_dim_accounts ac ON c.account_num = ac.account_num
                    LEFT JOIN deaian.trsh_dwh_dim_clients cl ON ac.client = cl.client_id) a
                WHERE oper_result = 'SUCCESS'
                AND res_3 = 'REJECT' AND res_3 IS NOT NULL
                AND res_2 = 'REJECT' AND res_2 IS NOT NULL
                AND res_1 = 'REJECT' AND res_1 IS NOT NULL
                AND amt_3 > amt_2 AND amt_3 IS NOT NULL
                AND amt_2 > amt_1 AND amt_2 IS NOT NULL
                AND amt_1 > amt AND amt_1 IS NOT NULL
              
                AND trans_date > (
                                select
					max_update_dt
                                FROM deaian.trsh_metadata
                                WHERE schema_name = 'deaian'
                                AND table_name = 'trsh_dwh_fact_transactions'
                                    )
   

             """)
  
conn1.commit()
# Закрываем соединение
cursor1.close()
conn1.close()

