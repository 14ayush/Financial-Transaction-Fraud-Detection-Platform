{{
  config(
    materialized = 'incremental',
    schema='silver',
    unique_key='device_id',
    incremental_strategy='merge'
    )
}}

-- here we need to maintain the device dimension like first seen , last seen, most used  ,country and etc

-- lets find the new_devices with new transactions

with new_transaction as (
    select * 
    from {{ ref('transaction_b') }}
    {% if is_incremental()%}
    where transaction_date > (select coalesce(max(last_seen_at),'1900-01-01') {{ this}})
    {% endif%}
),

-- lets find the new devices
affected_device as (
    select distinct device_id from new_transaction
),

--lets find the devices history 

device_history as (
    select b.*,
    row_number() over(partition by b.device_id order by b.transaction_date asc) as rn_first,
    row_number() over(partition by b.device_id order by b.transaction_date desc) as rn_last
    from {{ ref('transaction_b') }} as b
    inner join affected_device as a
    on a.device_id=b.device_id
),

--lest find the first and last seen 

first_seen as (
    select device_id, user_id as primary_user_id, device_type, transaction_date as first_seen_at
    from device_history where rn_first = 1
),

--last seen device 

last_seen as (
    select device_id, user_id as primary_user_id, device_type, transaction_date as last_seen_at
    from device_history where rn_last = 1
),

--aggregations

aggregation as (
    select device_id, count(*) as lifetime_transaction_count, count(distinct user_id) as distinct_user_count
    from device_history
    group by device_id
)

--main query 
select 
    f.device_id,
    f.device_type,
    f.primary_user_id,
    f.first_seen_at,
    l.last_seen_at,
    a.lifetime_transaction_count,
    a.distinct_user_count

from first_seen as f
join last_seen as l 
on f.device_id=l.device_id
join aggregation as a
on f.device_id=a.device_id 


