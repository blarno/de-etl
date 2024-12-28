insert into zxcv_stg_clients
select 
    client_id,
    last_name,
    first_name,
    patronymic as patrinymic,
    date_of_birth,
    passport_num,
    passport_valid_to,
    phone,
    create_dt,
    update_dt 
from info.clients c
where coalesce(update_dt,create_dt) > coalesce((select max(update_dt) from zxcv_dwh_dim_clients),'1000-01-01'::date);

insert into zxcv_dwh_dim_clients
SELECT client_id,
    last_name,
    first_name,
    patrinymic,
    date_of_birth,
    passport_num,
    passport_valid_to,
    phone,
    create_dt,
    update_dt 
FROM zxcv_stg_clients s 
WHERE NOT EXISTS (
    SELECT 1
    FROM zxcv_dwh_dim_clients t
    WHERE s.client_id = t.client_id)
AND s.client_id IS NOT NULL;

update zxcv_dwh_dim_clients
set 
    last_name = tmp.last_name,
    first_name = tmp.first_name,
    patrinymic = tmp.patrinymic,
    date_of_birth = tmp.date_of_birth,
    passport_num = tmp.passport_num,
    passport_valid_to = tmp.passport_valid_to,
    phone = tmp.phone,
    update_dt = '{insert_dt}'
from (
    select 
        stg.client_id,
        stg.last_name,
        stg.first_name,
        stg.patrinymic,
        stg.date_of_birth,
        stg.passport_num,
        stg.passport_valid_to,
        stg.phone
    from zxcv_stg_clients stg
    inner join zxcv_dwh_dim_clients tgt
    on stg.client_id = tgt.client_id
    where stg.last_name <> tgt.last_name or (stg.last_name is null and tgt.last_name is not null ) or ( stg.last_name is not null and tgt.last_name is null)
    or stg.first_name <> tgt.first_name or (stg.first_name is null and tgt.first_name is not null ) or ( stg.first_name is not null and tgt.first_name is null)
    or stg.patrinymic <> tgt.patrinymic or (stg.patrinymic is null and tgt.patrinymic is not null ) or ( stg.patrinymic is not null and tgt.patrinymic is null)
    or stg.date_of_birth <> tgt.date_of_birth or (stg.date_of_birth is null and tgt.date_of_birth is not null ) or ( stg.date_of_birth is not null and tgt.date_of_birth is null)
    or stg.passport_num <> tgt.passport_num or (stg.passport_num is null and tgt.passport_num is not null ) or ( stg.passport_num is not null and tgt.passport_num is null)
    or stg.passport_valid_to <> tgt.passport_valid_to or (stg.passport_valid_to is null and tgt.passport_valid_to is not null ) or ( stg.passport_valid_to is not null and tgt.passport_valid_to is null)
    or stg.phone <> tgt.phone or (stg.phone is null and tgt.phone is not null ) or ( stg.phone is not null and tgt.phone is null)
) tmp
where zxcv_dwh_dim_clients.client_id = tmp.client_id;

-- SCD2

insert into zxcv_dwh_dim_clients_hist
SELECT client_id,
    last_name,
    first_name,
    patrinymic,
    date_of_birth,
    passport_num,
    passport_valid_to,
    phone,
    create_dt,
    update_dt,
    '9999-12-31'::date as effective_to,
    TRUE as is_current
FROM zxcv_stg_clients s 
WHERE NOT EXISTS (
    SELECT 1
    FROM zxcv_dwh_dim_clients_hist t
    WHERE s.client_id = t.client_id)
AND s.client_id IS NOT NULL;

update zxcv_dwh_dim_clients_hist
set 
    effective_to = '{insert_dt}'::date - interval '1 day',
    is_current = FALSE
from (
    select 
        stg.client_id
    from zxcv_stg_clients stg
    inner join zxcv_dwh_dim_clients_hist tgt
    on stg.client_id = tgt.client_id
    and tgt.is_current = TRUE
    where stg.last_name <> tgt.last_name or (stg.last_name is null and tgt.last_name is not null ) or ( stg.last_name is not null and tgt.last_name is null)
    or stg.first_name <> tgt.first_name or (stg.first_name is null and tgt.first_name is not null ) or ( stg.first_name is not null and tgt.first_name is null)
    or stg.patrinymic <> tgt.patrinymic or (stg.patrinymic is null and tgt.patrinymic is not null ) or ( stg.patrinymic is not null and tgt.patrinymic is null)
    or stg.date_of_birth <> tgt.date_of_birth or (stg.date_of_birth is null and tgt.date_of_birth is not null ) or ( stg.date_of_birth is not null and tgt.date_of_birth is null)
    or stg.passport_num <> tgt.passport_num or (stg.passport_num is null and tgt.passport_num is not null ) or ( stg.passport_num is not null and tgt.passport_num is null)
    or stg.passport_valid_to <> tgt.passport_valid_to or (stg.passport_valid_to is null and tgt.passport_valid_to is not null ) or ( stg.passport_valid_to is not null and tgt.passport_valid_to is null)
    or stg.phone <> tgt.phone or (stg.phone is null and tgt.phone is not null ) or ( stg.phone is not null and tgt.phone is null)
) tmp
where zxcv_dwh_dim_clients_hist.client_id = tmp.client_id
  and zxcv_dwh_dim_clients_hist.is_current = TRUE;

insert into zxcv_dwh_dim_clients_hist
SELECT  stg.client_id,
        stg.last_name,
        stg.first_name,
        stg.patrinymic,
        stg.date_of_birth,
        stg.passport_num,
        stg.passport_valid_to,
        stg.phone,
        stg.create_dt,
        stg.update_dt,
        '9999-12-31'::date as effective_to,
        TRUE as is_current
FROM zxcv_stg_clients stg
LEFT JOIN (
    SELECT client_id, last_name, first_name, patrinymic, date_of_birth, passport_num, passport_valid_to, phone
    FROM zxcv_dwh_dim_clients_hist
    WHERE is_current = TRUE
) tgt
ON stg.client_id = tgt.client_id
WHERE (stg.last_name <> tgt.last_name or (stg.last_name is null and tgt.last_name is not null ) or ( stg.last_name is not null and tgt.last_name is null)
    or stg.first_name <> tgt.first_name or (stg.first_name is null and tgt.first_name is not null ) or ( stg.first_name is not null and tgt.first_name is null)
    or stg.patrinymic <> tgt.patrinymic or (stg.patrinymic is null and tgt.patrinymic is not null ) or ( stg.patrinymic is not null and tgt.patrinymic is null)
    or stg.date_of_birth <> tgt.date_of_birth or (stg.date_of_birth is null and tgt.date_of_birth is not null ) or ( stg.date_of_birth is not null and tgt.date_of_birth is null)
    or stg.passport_num <> tgt.passport_num or (stg.passport_num is null and tgt.passport_num is not null ) or ( stg.passport_num is not null and tgt.passport_num is null)
    or stg.passport_valid_to <> tgt.passport_valid_to or (stg.passport_valid_to is null and tgt.passport_valid_to is not null ) or ( stg.passport_valid_to is not null and tgt.passport_valid_to is null)
    or stg.phone <> tgt.phone or (stg.phone is null and tgt.phone is not null ) or ( stg.phone is not null and tgt.phone is null));