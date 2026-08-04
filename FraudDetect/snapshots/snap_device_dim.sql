{% snapshot snap_device_dim%}

{{ config(
    target_schema="snapshots",
    unique_key="device_id",
    strategy="check",
    check_cols=["device_type","primary_user_id"]
)}}

--creating device history

with device_history as (
    select device_id,device_type,user_id as primary_user_id,transaction_date
    from {{ ref('si_transaction_fact') }}
),

--finding the latest device 

latest_device as (
    select *,
    row_number() over(partition by device_id order by transaction_date desc) as row_num
    from device_history
)
-- main query 

select 
device_id,device_type,primary_user_id
from latest_device
where row_num=1

{% endsnapshot%}