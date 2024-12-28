drop table if exists zxcv_rep_fraud_passports;
create local temporary table zxcv_rep_fraud_passports
as
select trans_id,
       tr.trans_date as event_dt, 
	   cl.passport_num as passport,
	   CONCAT_WS(' ',cl.last_name,cl.first_name,cl.patrinymic) as fio,
	   cl.phone,
	   case when pas.passport_num is not null and tr.trans_date >= pas.entry_dt
	   			then 'Заблокированный паспорт' 
	   		when tr.trans_date > cl.passport_valid_to
	   			then 'Просроченный паспорт'
	   		when tr.trans_date > ac.valid_to
	   			then 'Недействующий договор'
	   end as event_type
from zxcv_dwh_fact_transactions tr
left join zxcv_dwh_dim_cards using(card_num)
left join zxcv_dwh_dim_accounts ac using(account_num) 
left join zxcv_dwh_dim_clients cl on cl.client_id = ac.client 
left join zxcv_dwh_fact_passport_blacklist pas on pas.passport_num = cl.passport_num
where tr.trans_date::date = '{insert_dt}';

insert into zxcv_rep_fraud
select  trans_id,
        event_dt,
        passport,
        fio,
        phone,
        event_type,
        '{insert_dt}' as report_dt
from zxcv_rep_fraud_passports tmp
WHERE NOT EXISTS (
    SELECT 1
    FROM zxcv_rep_fraud t
    WHERE tmp.trans_id = t.trans_id)
and tmp.event_type is not null;

--Как это в моем понимании должно работать
--Проверка городов за час до транзакции должна по хорошему выполняться на стороне сервиса, который изначально транзакцию апрувит
--Если сервис этого не делает, то тут нужно сделать двойную работу
-- -> Проверить города за час до транзакции, если их больше 1 -> транзакцию отклонить
-- -> Проверить города через час после транзакции, если их больше 1 -> транзакцию отклонить 
drop table if exists zxcv_rep_fraud_1_hour;
create local temporary table zxcv_rep_fraud_1_hour
as
with trans_slice as (
select trans_id,
       client,
       ter.terminal_city,
       trans_date - interval '1 hour' as start_period,
       trans_date,
       trans_date + interval '1 hour' as end_period
from zxcv_dwh_fact_transactions tr	 
left join zxcv_dwh_dim_cards using(card_num)
left join zxcv_dwh_dim_accounts ac using(account_num) 
left join zxcv_dwh_dim_terminals ter on tr.terminal = ter.terminal_id 
where trans_date <= '{insert_dt}'::timestamp + interval '1 day'
and  trans_date >= '{insert_dt}'::timestamp - interval '1 hour'
),
transactions_before as (
select ts1.client, 
       ts1.trans_id, 
	   count(distinct ts2.terminal_city) as dist_cities
from trans_slice ts1
left join trans_slice ts2 on ts1.client = ts2.client and ts1.trans_date between ts2.start_period and ts2.trans_date
group by 1,2
order by 3 asc
),
transactions_after as (
select ts1.client, 
       ts1.trans_id, 
	   count(distinct ts2.terminal_city) as dist_cities
from trans_slice ts1
left join trans_slice ts2 on ts1.client = ts2.client and ts1.trans_date between ts2.trans_date and ts2.end_period
group by 1,2
order by 3 asc
)
select 
distinct trans_id
from 
transactions_before
where dist_cities >= 2
UNION 
select 
distinct trans_id
from 
transactions_after
where dist_cities >= 2;

insert into zxcv_rep_fraud
select  tmp.trans_id,
        tr.trans_date as event_dt, 
	    cl.passport_num as passport,
	    CONCAT_WS(' ',cl.last_name,cl.first_name,cl.patrinymic) as fio,
	    cl.phone,
        'Совершение операций в разных городах за короткое время' as event_type,
        '{insert_dt}' as report_dt
from zxcv_rep_fraud_1_hour tmp
left join zxcv_dwh_fact_transactions tr using(trans_id)
left join zxcv_dwh_dim_cards using(card_num)
left join zxcv_dwh_dim_accounts ac using(account_num) 
left join zxcv_dwh_dim_clients cl on cl.client_id = ac.client 
WHERE NOT EXISTS (
    SELECT 1
    FROM zxcv_rep_fraud t
    WHERE tmp.trans_id = t.trans_id);

--Тут хотелось найти красивое решение, но всплыла проблема gaps-and-islands, которую я решить оказался не в силах
--поэтому вот упрощенный вариант
drop table if exists zxcv_rep_fraud_20_min;
create local temporary table zxcv_rep_fraud_20_min
as
with tmp as (
select trans_id,
	   card_num,
	   tr.amt,
	   trans_date,
	   oper_result,
	   lead(oper_result) over (partition by card_num order by trans_date desc) as previous_oper_result,
	   lead(oper_result,2) over (partition by card_num order by trans_date desc) as previous_1_oper_result,
	   lead(oper_result,3) over (partition by card_num order by trans_date desc) as previous_2_oper_result,
	   lead(amt) over (partition by card_num order by trans_date desc) as previous_oper_amt,
	   lead(amt,2) over (partition by card_num order by trans_date desc) as previous_1_oper_amt,
	   lead(amt,3) over (partition by card_num order by trans_date desc) as previous_2_oper_amt,
	   lead(trans_date,3) over (partition by card_num order by trans_date desc) as first_oper_trans_date
from zxcv_dwh_fact_transactions tr
left join zxcv_dwh_dim_cards using(card_num)
where oper_type in ('WITHDRAW','PAYMENT')
and  trans_date <= '{insert_dt}'::timestamp + interval '1 day'
and  trans_date >= '{insert_dt}'::timestamp - interval '20 minutes'
order by card_num, trans_date desc
)
select  distinct trans_id
from   	tmp
where  	oper_result = 'SUCCESS' 
and 	previous_oper_result = 'REJECT'
and     previous_1_oper_result = 'REJECT'
and     previous_2_oper_result = 'REJECT'
and     amt < previous_oper_amt
and     previous_oper_amt < previous_1_oper_amt
and     previous_1_oper_amt < previous_2_oper_amt
and     trans_date - first_oper_trans_date <= interval '20 minutes';

insert into zxcv_rep_fraud
select  tmp.trans_id,
        tr.trans_date as event_dt, 
	    cl.passport_num as passport,
	    CONCAT_WS(' ',cl.last_name,cl.first_name,cl.patrinymic) as fio,
	    cl.phone,
        'Попытка подбора суммы' as event_type,
        '{insert_dt}' as report_dt
from zxcv_rep_fraud_20_min tmp
left join zxcv_dwh_fact_transactions tr using(trans_id)
left join zxcv_dwh_dim_cards using(card_num)
left join zxcv_dwh_dim_accounts ac using(account_num) 
left join zxcv_dwh_dim_clients cl on cl.client_id = ac.client 
WHERE NOT EXISTS (
    SELECT 1
    FROM zxcv_rep_fraud t
    WHERE tmp.trans_id = t.trans_id);