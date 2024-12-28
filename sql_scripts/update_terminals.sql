insert into zxcv_dwh_dim_terminals
SELECT terminal_id,
    terminal_type,
    terminal_city,
    terminal_address,
    '{insert_dt}' as create_dt
FROM zxcv_stg_terminals s 
WHERE NOT EXISTS (
    SELECT 1
    FROM zxcv_dwh_dim_terminals t
    WHERE s.terminal_id = t.terminal_id)
AND s.terminal_id IS NOT NULL;

update zxcv_dwh_dim_terminals
set 
    terminal_type = tmp.terminal_type,
    terminal_city = tmp.terminal_city,
    terminal_address = tmp.terminal_address,
    update_dt = '{insert_dt}'
from (
    select 
        stg.terminal_id,
        stg.terminal_type,
        stg.terminal_city,
        stg.terminal_address
    from zxcv_stg_terminals stg
    inner join zxcv_dwh_dim_terminals tgt
    on stg.terminal_id = tgt.terminal_id
    where stg.terminal_type <> tgt.terminal_type or (stg.terminal_type is null and tgt.terminal_type is not null ) or ( stg.terminal_type is not null and tgt.terminal_type is null)
    or stg.terminal_city <> tgt.terminal_city or (stg.terminal_city is null and tgt.terminal_city is not null ) or ( stg.terminal_city is not null and tgt.terminal_city is null)
    or stg.terminal_address <> tgt.terminal_address or (stg.terminal_address is null and tgt.terminal_address is not null ) or ( stg.terminal_address is not null and tgt.terminal_address is null)
) tmp
where zxcv_dwh_dim_terminals.terminal_id = tmp.terminal_id;

--- SCD2

insert into zxcv_dwh_dim_terminals_hist
SELECT terminal_id,
       terminal_type,
       terminal_city,
       terminal_address,
       '{insert_dt}' as create_dt,
       '9999-12-31'::date as effective_to,
       TRUE as is_current
FROM zxcv_stg_terminals s 
WHERE NOT EXISTS (
    SELECT 1
    FROM zxcv_dwh_dim_terminals_hist t
    WHERE s.terminal_id = t.terminal_id)
AND s.terminal_id IS NOT NULL;

update zxcv_dwh_dim_terminals_hist
set 
    effective_to = '{insert_dt}'::date - interval '1 day',
    is_current = FALSE
from (
    select 
        stg.terminal_id
    from zxcv_stg_terminals stg
    inner join zxcv_dwh_dim_terminals_hist tgt
    on stg.terminal_id = tgt.terminal_id
    and tgt.is_current = TRUE
    where stg.terminal_type <> tgt.terminal_type 
          or (stg.terminal_type is null and tgt.terminal_type is not null)
          or (stg.terminal_type is not null and tgt.terminal_type is null)
          or stg.terminal_city <> tgt.terminal_city 
          or (stg.terminal_city is null and tgt.terminal_city is not null)
          or (stg.terminal_city is not null and tgt.terminal_city is null)
          or stg.terminal_address <> tgt.terminal_address 
          or (stg.terminal_address is null and tgt.terminal_address is not null)
          or (stg.terminal_address is not null and tgt.terminal_address is null)
) tmp
where zxcv_dwh_dim_terminals_hist.terminal_id = tmp.terminal_id
  and zxcv_dwh_dim_terminals_hist.is_current = TRUE;

insert into zxcv_dwh_dim_terminals_hist
SELECT  stg.terminal_id,
        stg.terminal_type,
        stg.terminal_city,
        stg.terminal_address,
        '{insert_dt}' as create_dt,
        '9999-12-31'::date as effective_to,
        TRUE as is_current
FROM zxcv_stg_terminals stg
LEFT JOIN (
    SELECT terminal_id, terminal_type, terminal_city, terminal_address
    FROM zxcv_dwh_dim_terminals_hist
    WHERE is_current = TRUE
) tgt
ON stg.terminal_id = tgt.terminal_id
WHERE (stg.terminal_type <> tgt.terminal_type 
       or (stg.terminal_type is null and tgt.terminal_type is not null)
       or (stg.terminal_type is not null and tgt.terminal_type is null)
       or stg.terminal_city <> tgt.terminal_city 
       or (stg.terminal_city is null and tgt.terminal_city is not null)
       or (stg.terminal_city is not null and tgt.terminal_city is null)
       or stg.terminal_address <> tgt.terminal_address 
       or (stg.terminal_address is null and tgt.terminal_address is not null)
       or (stg.terminal_address is not null and tgt.terminal_address is null));