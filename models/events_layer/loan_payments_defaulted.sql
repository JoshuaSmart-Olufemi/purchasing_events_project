with base as (

    select * from {{ ref('stg_motherduck__loans_data') }}

)

, event as (

    select 
    *
    , row_number() over (partition by coalesce (activity, customer, anonymous_customer_id) order by ts asc) as activity_occurrence
    , lead(ts) over (partition by coalesce (activity, customer, anonymous_customer_id) order by ts asc) as activity_repeated_at
    from base
    where activity = 'loan_payment_defaulted'
    order by ts  
)

select * from event 