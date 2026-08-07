-- lets create the feature for the ML model 
--The machine learning model doesn't understand business concepts like "new device" or "impossible travel."

--It only sees numbers.

{{ config(
    materialized='incremental',
    unique_id='transaction_id',
    incremental_strategy='merge',
    schema='gold',
)}}


--CTE for new transaction 

with new_transaction as(
    select * 
    from {{ ref('si_transaction_fact')}}
    {% if is_incremental() %}
    where transaction_date > (select coalesce(max(last_updated_at),'1900-01-01') {{ this}})
    {% endif%}
),

--checking for new customer 

affected_users as (
    select distinct user_id from new_transaction
),

--full history of user 

user_history as (
    select f.* 
    from {{ ref('si_transaction_fact')}} as f
    inner join affected_users as a
    on f.user_id=a.user_id
),

-- most important and critical usecase for SCD 2 . Let us assume customer1:
-- lives in india till march 15 after that he shift to USA. now if transaction is happen on feb 10 and we need the billing country of customer
-- we will get USA because latest country is USA if we didnot use the SCD 2 however customer was in India during feb.
-- as a result we may get the incosistency in billing country != IP_address of country . which cause the fraud flag in ML model which is incorrect .

-- so to resolve this we use the SCD 2 to join the customer to get correct country

billing_country_at_txn_time as (
    select h.*,s.billing_country as billing_country_at_tx_time
    from user_history h
    left join {{ ref('snap_users_dim')}} as s
    on h.user_id=s.user_id
    and h.transaction_date >= s.dbt_valid_from 
    and (h.transaction_date <s.dbt_valid_to or s.dbt_valid_to is null)
),

-- now its time to get the other info's and the features 

windowed as (
    select *,
        lag(transaction_date) over(partition by user_id order by transaction_date ) as prev_transaction_date,
        lag(location_lat) over (partition by user_id order by transaction_date) as prev_lat,
        lag(location_lon) over(partition by user_id order by transaction_date) as prev_lon,

        --finding the count of all transaction made in 1 hour --(velocity) and first device used for transaction
        count(*) over(
            partition by user_id 
            order by transaction_date
            range between interval 1 hour preceding and current row
        )as velocity_1hr_inclusive,
        min(transaction_date) over (partition by user_id,device_id) as device_first_seen_at
    from billing_country_at_txn_time


),

-- other features 

-- distance calculation 
--Feature: Impossible Travel
--Checks whether the user appears to have traveled between two transaction locations at an impossible speed.

distance_calc as (
    select *,
        case
                when prev_lat is not null then
                    2 * 6371 * asin(sqrt(
                        pow(sin(radians(location_lat - prev_lat) / 2), 2) +
                        cos(radians(prev_lat)) * cos(radians(location_lat)) *
                        pow(sin(radians(location_lon - prev_lon) / 2), 2)
                    )) else null
        end as distance_from_prev_km,
        case 
                when prev_transaction_date is not null then
                greatest(unix_timestamp(transaction_date) - unix_timestamp(prev_transaction_date), 1) / 3600.0
            else null
        end as hours_since_prev_tx
    from windowed
),

--feature calculation 

feature_calc as (
    select 
        d.transaction_id,
        d.user_id,
        d.merchant_id,

        --calculating the feature amount zscore
        (d.transaction_amount-b.avg_amount_30d)/ nullif(b.stddev_amount_30d,0) as amount_zscore,

        ---1hr velocity
        greatest(d.velocity_1hr_inclusive - 1, 0) as velocity_1hr,

        ---impossible travel flag

        
        case when d.transaction_date = d.device_first_seen_at then 1 else 0 end as new_device_flag,


        -- device amount percentile 

        d.transaction_amount / nullif(b.p90_amount_30d, 0) as device_amount_percentile,

        -- off_hours_flags
        case
            when hour(d.transaction_date) not between b.earliest_active_hour and b.latest_active_hour
            then 1 else 0
        end as off_hours_flag,

        --location_mismatch_flag

        case when d.ip_country != d.billing_country_at_tx_time then 1 else 0 end as location_mismatch_flag,

        m.fraud_rate_90d as merchant_risk_rate,

        d.is_fraud_label,
        d.transaction_date
    
    from distance_calc as d 
    left join {{ ref('go_user_behaviour')}} as b
    on b.user_id=d.user_id
    left join {{ ref('go_merchant_risk')}} as m
    on d.merchant_id=m.merchant_id
)


--main query 

select *
from feature_calc
-- window functions needed the wider history for context, but we only
-- persist feature rows for transactions that are actually new this run
where transaction_id in (select transaction_id from new_transaction)
