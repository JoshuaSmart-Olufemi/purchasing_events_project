--LAST BEFORE: last ongoing loan BEFORE loan is repaid
    select  
    customer
    , max(ts) as repaid_ts
    from {{ ref('loan_stream') }} as r
 
    INNER JOIN
    (
    select
    customer
    , max(ts) as ongoing_ts
    from {{ ref('loan_stream') }} 
    group by 1
    ) as o
    
    ON  

    (

    r.customer = o.customer
    and o.ongoing_ts < r.repaid_ts 

    )

    WHERE 
    (
    activity = 'loan_payment_repaid' AND
    activity = 'loan_payment_ongoing'
    )
    GROUP BY 1
    
select * from final 

--FIRST BEFORE: last ongoing loan BEFORE loan is repaid
with loans_repaid as (

    select  
    customer
    , min(ts) as repaid_ts
    from {{ ref('loan_stream') }}
    where activity = 'loan_payment_repaid'
    group by 1

)

, loans_ongoing as (

    select
    customer
    , min(ts) as ongoing_ts
    from {{ ref('loan_stream') }} 
    where activity = 'loan_payment_ongoing'
    group by 1

)

, final as (

    select o.customer
    , o.ongoing_ts
    , r.repaid_ts
    from loans_repaid as r
    inner join 
    loans_ongoing as o 
    on r.customer = o.customer
    and o.ongoing_ts < r.repaid_ts 

)

select * from final 