with loans_defaulted as (

    select * from {{ ref('loan_payments_defaulted') }}

)

, loans_ongoing as (

    select * from {{ ref('loan_payments_ongoing') }}

)

, loans_repaid as (

    select * from {{ ref('loan_payments_repaid') }}

)

, final as (

    select * from loans_defaulted
    union all
    select * from loans_ongoing
    union all
    select * from loans_repaid

)

select * from final 