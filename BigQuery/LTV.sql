SELECT a.productID, SUM(a.repated_trn_count*avg_amount*avg_retention_time) as _LTV
from
(select p.id as customerID,product.id as productID,
count(distinct invoices.id) as repated_trn_count,round(avg(invoices.amount),2) as avg_amount,
(EXTRACT(Year FROM p.expired_at)*12 + EXTRACT(MONTH FROM p.expired_at)) - (EXTRACT(Year FROM p.started_at)*12 + EXTRACT(MONTH FROM p.started_at)) as
avg_retention_time  
from `holy-water-348217.Product.Product` p, unnest(invoices) invoices
where invoices.status = 'success'
group by product.id, p.id,p.started_at,p.expired_at
having (EXTRACT(Year FROM p.expired_at)*12 + EXTRACT(MONTH FROM p.expired_at)) - (EXTRACT(Year FROM p.started_at)*12 + EXTRACT(MONTH FROM p.started_at)) >0) a
group by a.productID