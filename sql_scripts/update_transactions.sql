insert into zxcv_dwh_fact_transactions
select  trans_id, 
        trans_date, 
        card_num,
        oper_type,
        amt,
        oper_result,
        terminal
from zxcv_stg_transactions;