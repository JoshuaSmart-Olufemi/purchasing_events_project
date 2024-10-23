--aggregate before: how many loans were ongoing before being repaid
-- or how many loans did each customer have ongoing before they were repaid
--
--count of loans ongoing before repaid: 5484
--count of loans ongoing before first installment of repayment: 0
--how many loans did each customer have ongoing before they were repaid?: ++
-- how mnay loans did each customer have ongoing before a first installment of repayments?: 0?

--count of loans ongoing after repayment
-- how many loans did each customer have ongoing after a first installment of repayments? : 1
-- how many loans did each customer have ongoing after a second installment of repayments? : 0
select count(*) count_of_loans
, repaid.customer
from {{ ref('loan_stream') }} as repaid
INNER JOIN {{ ref('loan_stream') }} as ongoing 
ON
(
    ongoing.customer = repaid.customer AND
    ongoing.ts < repaid.ts
)
WHERE
(
    repaid.activity = 'loan_payment_repaid' AND 
    repaid.activity_occurrence = 1 AND
    ongoing.activity = 'loan_payment_ongoing'
)
GROUP BY 2
