def return_sql():
    sql = """
select distinct fs2.email
	, ds."name" as store
	, fs2.created_at as order_date
	, fs2.order_number
	, pp.partner_id as parent_co
	, pp.vendor as brand
	, fs2.product_id as item_id
	, pp.title as item_name
	, fs2.price
	, fs2.quantity
	, fs2.engraved_quantity
	, fs2.price * fs2.quantity as line_sub_total
	, fs2.order_status
from reporting.fact_sales fs2
left join reporting.dim_storefront ds on ds.id = fs2.storefront_id
left join reporting.product pp on pp.product_id = fs2.product_id
order by order_date desc
;
    """
    return sql
