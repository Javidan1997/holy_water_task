select three_days.productID,three_days.conversion_rate as three_day_conversion,seven_days.conversion_rate as seven_days_conversion,fourteen_days.conversion_rate as fourteen_days_conversion
from
(select case when a.productID is null then b.productID else a.productID end productID,
count(distinct a.customerId) as oneTimePurchase, count(distinct b.customerId) as multipleTimePurchase,
round((count(distinct b.customerId)/(count(distinct a.customerId)+count(distinct b.customerId))) * 100,2) as conversion_rate
from
(select p.id as customerId,product.id as productID,
count(distinct invoices.id) as succes_invoice, count(distinct p.id) as customer_count
from `holy-water-348217.Product.Product` p join unnest(invoices) invoices on p.id=p.id
where invoices.status = 'success' and   p.id = p.id and invoices.created_at between p.started_at and DATE_ADD( p.started_at, INTERVAL 3 DAY)
group by p.id,product.id
having count(distinct invoices.id)=1) a
full join 
(select p.id as customerId,product.id as productID,
count(distinct invoices.id) as succes_invoice, count(distinct p.id) as customer_count
from `holy-water-348217.Product.Product` p join unnest(invoices) invoices on p.id=p.id
where invoices.status = 'success'  and p.id = p.id and invoices.created_at between p.started_at and DATE_ADD( p.started_at, INTERVAL 3 DAY)
group by p.id,product.id
having count(distinct invoices.id)>1) b on a.productID = b.productID 
group by a.productID,b.productID) three_days
full join
(select case when a.productID is null then b.productID else a.productID end productID,
count(distinct a.customerId) as oneTimePurchase, count(distinct b.customerId) as multipleTimePurchase,
round((count(distinct b.customerId)/(count(distinct a.customerId)+count(distinct b.customerId))) * 100,2) as conversion_rate
from
(select p.id as customerId,product.id as productID,
count(distinct invoices.id) as succes_invoice, count(distinct p.id) as customer_count
from `holy-water-348217.Product.Product` p join unnest(invoices) invoices on p.id=p.id
where invoices.status = 'success' and   p.id = p.id and invoices.created_at between p.started_at and DATE_ADD( p.started_at, INTERVAL 14 DAY) 
group by p.id,product.id
having count(distinct invoices.id)=1) a
full join 
(select p.id as customerId,product.id as productID,
count(distinct invoices.id) as succes_invoice, count(distinct p.id) as customer_count
from `holy-water-348217.Product.Product` p join unnest(invoices) invoices on p.id=p.id
where invoices.status = 'success'  and p.id = p.id and invoices.created_at  between p.started_at and DATE_ADD( p.started_at, INTERVAL 14 DAY) 
group by p.id,product.id
having count(distinct invoices.id)>1) b on a.productID = b.productID 
group by  a.productID,b.productID
) fourteen_days on three_days.productID = fourteen_days.productID
full join
(select case when a.productID is null then b.productID else a.productID end productID,
count(distinct a.customerId) as oneTimePurchase, count(distinct b.customerId) as multipleTimePurchase,
round((count(distinct b.customerId)/(count(distinct a.customerId)+count(distinct b.customerId))) * 100,2) as conversion_rate
from
(select p.id as customerId,product.id as productID,
count(distinct invoices.id) as succes_invoice, count(distinct p.id) as customer_count
from `holy-water-348217.Product.Product` p join unnest(invoices) invoices on p.id=p.id
where invoices.status = 'success' and   p.id = p.id and invoices.created_at between p.started_at and DATE_ADD( p.started_at, INTERVAL 7 DAY) 
group by p.id,product.id
having count(distinct invoices.id)=1) a
full join 
(select p.id as customerId,product.id as productID,
count(distinct invoices.id) as succes_invoice, count(distinct p.id) as customer_count
from `holy-water-348217.Product.Product` p join unnest(invoices) invoices on p.id=p.id
where invoices.status = 'success'  and p.id = p.id and invoices.created_at  between p.started_at and DATE_ADD( p.started_at, INTERVAL 7 DAY) 
group by p.id,product.id
having count(distinct invoices.id)>1) b on a.productID = b.productID 
group by  a.productID,b.productID
) seven_days on three_days.productID = seven_days.productID