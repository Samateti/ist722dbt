with stg_orders as 
(
    select 
        {{ dbt_utils.generate_surrogate_key(['employeeid']) }} as employeekey, 
        {{ dbt_utils.generate_surrogate_key(['customerid']) }} as customerkey,
        replace(to_date(orderdate)::varchar,'-','')::int as orderdatekey,
        orderid as orderkey
    from {{source('northwind','Orders')}}
),
stg_order_details as
(
    select 
        orderid,Quantity,
        {{ dbt_utils.generate_surrogate_key(['productid']) }} as productkey,
        sum(Quantity*UnitPrice) as extendedpriceamount, 
        sum(Quantity*UnitPrice*Discount) as discountamount,
        sum(Quantity*UnitPrice*(1-Discount)) as soldamount
    from {{source('northwind','Order_Details')}}
    group by orderid,Quantity,productkey
)
select  
  o.employeekey, o.customerkey, o.orderdatekey,od.productkey,o.orderkey,od.Quantity,od.extendedpriceamount,od.discountamount,od.soldamount

from stg_orders o
    join stg_order_details od on o.orderkey = od.orderid