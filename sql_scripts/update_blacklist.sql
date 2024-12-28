delete 
from zxcv_dwh_fact_passport_blacklist
where date_trunc('month', entry_dt) in (select min(entry_dt) from zxcv_stg_blacklist);
insert into zxcv_dwh_fact_passport_blacklist
select  passport_num,
        entry_dt
from zxcv_stg_blacklist;