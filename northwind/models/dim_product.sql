with stg_products as (
    select
        {{ dbt_utils.generate_surrogate_key(['productid']) }} as productkey,
        productid,
        {{ dbt_utils.generate_surrogate_key(['supplierid']) }} as supplierkey,
        supplierid,
        categoryid
    from {{ source('northwind', 'Products') }}
),
stg_categories as (
    select * from {{ source('northwind', 'Categories') }}
)
select 
    stg.productkey,
    stg.productid,
    stg.supplierkey,
    c.categoryname as categoryname,
    c.description as categorydescription
from stg_products stg
left join stg_categories c on stg.categoryid = c.categoryid
