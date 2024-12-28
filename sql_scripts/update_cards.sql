insert into zxcv_stg_cards
select  card_num,
        account as account_num,
        create_dt,
        update_dt 
from info.cards c
where coalesce(update_dt,create_dt) > coalesce((select max(update_dt) from zxcv_dwh_dim_cards),'1000-01-01'::date);

insert into zxcv_dwh_dim_cards
SELECT  card_num,
        account_num,
        create_dt,
        update_dt 
FROM zxcv_stg_cards s 
WHERE NOT EXISTS (
    SELECT 1
    FROM zxcv_dwh_dim_cards t 
    WHERE s.card_num = t.card_num)
AND s.card_num IS NOT NULL;

update zxcv_dwh_dim_cards
set 
    account_num = tmp.account_num,
    update_dt = '{insert_dt}'
from (
    select 
        stg.card_num,
        stg.account_num
    from zxcv_stg_cards stg
    inner join zxcv_dwh_dim_cards tgt
    on stg.card_num = tgt.card_num
    where stg.account_num <> tgt.account_num or (stg.account_num is null and tgt.account_num is not null ) or ( stg.account_num is not null and tgt.account_num is null)
) tmp
where zxcv_dwh_dim_cards.card_num = tmp.card_num;

---SCD2

insert into zxcv_dwh_dim_cards_hist
SELECT  card_num,
        account_num,
        create_dt,
        update_dt,
        '9999-12-31'::date as effective_to,
        TRUE as is_current
FROM zxcv_stg_cards s 
WHERE NOT EXISTS (
    SELECT 1
    FROM zxcv_dwh_dim_cards_hist t 
    WHERE s.card_num = t.card_num
)
AND s.card_num IS NOT NULL;

update zxcv_dwh_dim_cards_hist
set 
    effective_to = '{insert_dt}'::date - interval '1 day',
    is_current = FALSE
from (
    select 
        stg.card_num
    from zxcv_stg_cards stg
    inner join zxcv_dwh_dim_cards_hist tgt
    on stg.card_num = tgt.card_num
    and tgt.is_current = TRUE
    where stg.account_num <> tgt.account_num 
          or (stg.account_num is null and tgt.account_num is not null)
          or (stg.account_num is not null and tgt.account_num is null)
) tmp
where zxcv_dwh_dim_cards_hist.card_num = tmp.card_num
  and zxcv_dwh_dim_cards_hist.is_current = TRUE;

insert into zxcv_dwh_dim_cards_hist
SELECT  stg.card_num,
        stg.account_num,
        stg.create_dt,
        stg.update_dt,
        '9999-12-31'::date as effective_to,
        TRUE as is_current
FROM zxcv_stg_cards stg
LEFT JOIN (
    SELECT card_num, account_num
    FROM zxcv_dwh_dim_cards_hist
    WHERE is_current = TRUE
) tgt
ON stg.card_num = tgt.card_num
WHERE (stg.account_num <> tgt.account_num 
       or (stg.account_num is null and tgt.account_num is not null)
       or (stg.account_num is not null and tgt.account_num is null));
