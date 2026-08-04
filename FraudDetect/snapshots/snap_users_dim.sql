{% snapshot snap_users_dim%}
{{
  config(
    target_schema="snapshots",
    unique_key="user_id",
    strategy="check",
    check_cols=["billing_country","preferred_payment_method"],
    )


}}

-- creating the users_history 

with users_history as(
    select user_id,billing_country,payment_method as preferred_payment_method,transaction_date
    from {{ ref('si_transaction_fact') }}
),

--now finding the latest record for each user_id
latest_users as (
    select *,
    row_number() over(partition by user_id order by transaction_date desc) as row_num
    from users_history
)

--main query 

select user_id,billing_country,preferred_payment_method
from latest_users
where row_num=1

{% endsnapshot%}