with 

source as (

    select * from {{ source('motherduck', 'loans_data') }}

),

renamed as (

    select
        listedonutc,
        loanid as activity_id,
        loanapplicationstarteddate as ts,
        partyid as customer,
        _status as activity,
        cast(principalpaymentsmade as numeric) + cast(interestandpenaltypaymentsmade as numeric) revenue_impact

    from source

),

activities as (
    
    select  
    activity_id
    , ts
    , customer
    , case when activity = 'Current' then 'loan_payment_ongoing'
            when activity = 'Late' then 'loan_payment_defaulted'
            when activity = 'Repaid' then 'loan_payment_repaid'
      end as activity
    , revenue_impact
    from renamed
)

select * from activities

