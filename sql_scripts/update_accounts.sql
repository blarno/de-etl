insert into zxcv_stg_accounts
select  account as account_num,
        valid_to,
        client,
        create_dt,
        update_dt 
from info.accounts c
where coalesce(update_dt,create_dt) > coalesce((select max(update_dt) from zxcv_dwh_dim_cards),'1000-01-01'::date);

insert into zxcv_dwh_dim_accounts
SELECT  account_num,
        valid_to,
        client,
        create_dt,
        update_dt
FROM zxcv_stg_accounts s 
WHERE NOT EXISTS (
    SELECT 1
    FROM zxcv_dwh_dim_accounts t 
    WHERE s.account_num = t.account_num)
AND s.account_num IS NOT NULL;

update zxcv_dwh_dim_accounts
set 
    valid_to = tmp.valid_to,
    client = tmp.client,
    update_dt = '{insert_dt}'
from (
    select 
        stg.account_num,
        stg.valid_to,
        stg.client
    from zxcv_stg_accounts stg
    inner join zxcv_dwh_dim_accounts tgt
    on stg.account_num = tgt.account_num
    where stg.valid_to <> tgt.valid_to or (stg.valid_to is null and tgt.valid_to is not null ) or ( stg.valid_to is not null and tgt.valid_to is null)
    or stg.client <> tgt.client or (stg.client is null and tgt.client is not null ) or ( stg.client is not null and tgt.client is null)
) tmp
where zxcv_dwh_dim_accounts.account_num = tmp.account_num;

---SCD2

insert into zxcv_dwh_dim_accounts_hist
SELECT  account_num,
        valid_to,
        client,
        create_dt,
        update_dt,
        '9999-12-31'::date as effective_to,
        TRUE as is_current
FROM zxcv_stg_accounts s 
WHERE NOT EXISTS (
    SELECT 1
    FROM zxcv_dwh_dim_accounts_hist t 
    WHERE s.account_num = t.account_num
)
AND s.account_num IS NOT NULL;

update zxcv_dwh_dim_accounts_hist
set 
    effective_to = '{insert_dt}'::date - interval '1 day',
    is_current = FALSE
from (
    select 
        stg.account_num
    from zxcv_stg_accounts stg
    inner join zxcv_dwh_dim_accounts_hist tgt
    on stg.account_num = tgt.account_num
    and tgt.is_current = TRUE
    where stg.valid_to <> tgt.valid_to 
          or (stg.valid_to is null and tgt.valid_to is not null)
          or (stg.valid_to is not null and tgt.valid_to is null)
          or stg.client <> tgt.client 
          or (stg.client is null and tgt.client is not null)
          or (stg.client is not null and tgt.client is null)
) tmp
where zxcv_dwh_dim_accounts_hist.account_num = tmp.account_num
  and zxcv_dwh_dim_accounts_hist.is_current = TRUE;

insert into zxcv_dwh_dim_accounts_hist
SELECT  stg.account_num,
        stg.valid_to,
        stg.client,
        stg.create_dt,
        stg.update_dt,
        '9999-12-31'::date as effective_to,
        TRUE as is_current
FROM zxcv_stg_accounts stg
LEFT JOIN (
    SELECT account_num, valid_to, client
    FROM zxcv_dwh_dim_accounts_hist
    WHERE is_current = TRUE
) tgt
ON stg.account_num = tgt.account_num
WHERE (stg.valid_to <> tgt.valid_to 
       or (stg.valid_to is null and tgt.valid_to is not null)
       or (stg.valid_to is not null and tgt.valid_to is null)
       or stg.client <> tgt.client 
       or (stg.client is null and tgt.client is not null)
       or (stg.client is not null and tgt.client is null));