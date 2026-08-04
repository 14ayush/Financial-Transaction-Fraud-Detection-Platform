{{
  config(
    materialized = 'incremental',
    unique_key = 'user_id',
    incremental_strategy = 'merge',
    schema ='silver'
    )
}}

--we are maintaining the user dimension like first seen , last seen, most purchase ,country and etc

-- lets find the new_customers with new transactions

with new_transaction as (
    select * 
    from {{ ref('transaction_b') }}
    {% if is_incremental()%}
    where transaction_date > (select coalesce(max(last_seen_at),'1900-01-01') from {{ this }})
    {% endif %}
),
-- now lets find the distinct users from the latest transaction 
affected_user as (
    select distinct user_id from new_transaction
),

--lets rank the customers based on there transaction date which help to find the user_history 

user_history as (
    select b.*,
    row_number() over(partition by b.user_id order by b.transaction_date desc) as rn
    from {{ ref('transaction_b') }} as b 
    join affected_user as a 
    on b.user_id=a.user_id
),

-- now time to find the latest attribute used by user 

latest_attributes as(
    select user_id, billing_country, payment_method as preferred_payment_method
    from user_history 
    where rn=1
),

--its time to find the aggregation

aggregates as (
    select user_id,
    min(transaction_date) as first_seen_at,
    max(transaction_date) as last_seen_at,
    count(*) as lifetime_transaction_count
    from user_history
    group by user_id

)
--it is our main query 
select
    a.user_id, a.first_seen_at, a.last_seen_at, a.lifetime_transaction_count,
    l.billing_country, l.preferred_payment_method
from aggregates a
join latest_attributes l on a.user_id = l.user_id 