select case when a.productID is null then b.productID else a.productID end productID,
count(distinct a.customerId) as oneTimePurchase, count(distinct b.customerId) as multipleTimePurchase,
round((count(distinct b.customerId)/(count(distinct a.customerId)+count(distinct b.customerId))) * 100,2) as conversion_rate
from
(select p.id as customerId,product.id as productID,
count(distinct invoices.id) as succes_invoice, count(distinct p.id) as customer_count
from `holy-water-348217.Product.Product` p join unnest(invoices) invoices on p.id=p.id
where invoices.status = 'success' and   p.id = p.id
group by p.id,product.id
having count(distinct invoices.id)=1) a
full join 
(select p.id as customerId,product.id as productID,
count(distinct invoices.id) as succes_invoice, count(distinct p.id) as customer_count
from `holy-water-348217.Product.Product` p join unnest(invoices) invoices on p.id=p.id
where invoices.status = 'success'  and p.id = p.id
group by p.id,product.id
having count(distinct invoices.id)>1) b on a.productID = b.productID 
group by  a.productID,b.productID