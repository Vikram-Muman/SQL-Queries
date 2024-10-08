/* For each Customer Reporting Group calculate the difference between the YTD sales and YTD budget */

with sales as(
select
  sum(a.invoiced_amount) as YTD_sales,
  c.customer_reporting_group
from powerbi.model.fact_sales_order_lines a
left join powerbi.model.dim_date b on a.invoice_date_key = b.date_key
left join powerbi.model.dim_customers c on a.sold_to_customer_key = c.customers_key
where b.fiscal_year = 2025 and b.date <= GETDATE()
group by c.customer_reporting_group
),

budget as (
select
  sum(a.net_sales_value) as YTD_budget,
  c.customer_reporting_group
from powerbi.model.fact_sales_budget a
left join powerbi.model.dim_date b on a.date_key = b.date_key
left join powerbi.model.dim_customers c on a.customer_key = c.customers_key
where b.fiscal_year = 2025 and b.date <= GETDATE()
group by c.customer_reporting_group
),

customers as (
select 
  customer_reporting_group
from powerbi.model.dim_customers
group by customer_reporting_group
)

select
  a.customer_reporting_group,
  sum(b.YTD_sales) as YTD_Sales,
  sum(c.YTD_budget) as YTD_Budget,
  sum(b.YTD_sales) - sum(c.YTD_budget) as Budget_Variance
from customers a
left join sales b on a.customer_reporting_group = b.customer_reporting_group
left join budget c on a.customer_reporting_group = c.customer_reporting_group
group by a.customer_reporting_group
order by Budget_Variance asc

 

 

/* Find the cumulative value and percentage of each supplier within each supplier country in this Fiscal Year (FY2025) */

with data as (
select
  c.supplier_name,
  c.supplier_country,
  sum(a.delivered_amount) AS delivered_value
from powerbi.model.fact_purchase_lines a
left join powerbi.model.dim_date b ON a.delivery_date_key = b.date_key
left join powerbi.model.dim_suppliers c ON a.buy_from_supplier_key = c.suppliers_key
where b.fiscal_year = 2025
group by c.supplier_name, c.supplier_country
),
  
total_by_country AS (
select
  supplier_country,
  sum(delivered_value) AS total_value
from data
group by supplier_country
)
  
select
  a.supplier_name,
  a.supplier_country,
  sum(a.delivered_value) OVER (PARTITION BY a.supplier_country ORDER BY a.delivered_value desc) AS cumulative_value,
  sum(a.delivered_value) OVER (PARTITION BY a.supplier_country ORDER BY a.delivered_value desc) / b.total_value * 100 AS cumulative_percentage
from data a
left join total_by_country b ON a.supplier_country = b.supplier_country
order by a.supplier_country, cumulative_value

 

 

/* Find the top 3 suppliers with largest delivered value, by fiscal year. Include their total delivered value, total delivered quantity and their #1 supplied component */

with total_invoiced_value AS (
select
  b.supplier_name,
  d.item_id,
  sum(a.invoiced_amount) as Total_invoiced_value,
  c.fiscal_year
from powerbi.model.fact_purchase_lines a
left join powerbi.model.dim_suppliers b on a.buy_from_supplier_key =b.suppliers_key
left join powerbi.model.dim_date c on a.delivery_date_key = c.date_key
left join powerbi.model.dim_item d on a.item_key = d.item_key
left join powerbi.model.dim_item_groups e on d.item_group_key = e.item_groups_key
where fiscal_year IS NOT NULL
group by supplier_name, fiscal_year, d.item_id
),

Rank_by_component AS (
select
   supplier_name,
   item_id,
   total_invoiced_value,
   fiscal_year,
   rank() OVER (partition By supplier_name, fiscal_year order by total_invoiced_value DESC) AS 'rank_by_fiscal_year'
from total_invoiced_value
),

total_Amount AS (
select
  supplier_name,
  sum(total_invoiced_value) As 'Total_Amount',
  fiscal_year
from Rank_by_component
group by supplier_name,fiscal_year
),

Final_version AS (
select
   supplier_name,
   total_amount,
   fiscal_year,
   rank() OVER (partition By fiscal_year order by total_amount desc) as 'rank_by_year'
from total_amount
)

select
    a.supplier_name,
    a.fiscal_year,
    a.total_amount,
    rank_by_year,
    b.item_id as '#1_component'
from  Final_version a
left join Rank_by_component  b on  a.supplier_name =b.supplier_name and a.fiscal_year=b.fiscal_year and b.rank_by_fiscal_year=1
where rank_by_year in(1,2,3)
order by fiscal_year 

 

 

/* For all products sold in the last 10 years (excluding export) list all BOM components that have been sold as spares including product sold volume, spares sold volume and last sold date as a spare */

with products as ( -- products sold since 2015
select
  b.item_id as product_id,
  b.description as product,
  sum(a.invoiced_quantity) as product_sold_quantity,
  sum(a.invoiced_amount) as product_sold_value,
  max(a.invoice_date) as product_last_sold_date,
  d.product_group_description as product_group,
  d.[description] as item_group,
  b.product_type_description as product_family
from powerbi.model.fact_sales_order_lines a
left join powerbi.model.dim_item b on a.item_key = b.item_key
left join powerbi.model.dim_item_groups d on b.item_group_key = d.item_groups_key
left join powerbi.model.dim_customers e on a.invoice_to_customer_key = e.customers_key
where
  d.product_flag = 'product'
  and a.status = 'invoiced'
  and a.invoiced_quantity <> 0
  and e.sector_description <> 'Export'
group by
  b.item_id,
  b.description,
  d.product_group_description,
  d.description,
  b.product_type_description
),

spares as ( -- spares
select
  b.item_id as spare_id,
  b.description as spare,
  sum(a.invoiced_quantity) as spare_sold_quantity,
  sum(a.invoiced_amount) as spare_sold_value,
  max(a.invoice_date) as spare_last_sold_date
from powerbi.model.fact_sales_order_lines a
left join powerbi.model.dim_item b ON a.item_key = b.item_key
left join powerbi.model.dim_item_groups c ON b.item_group_key = c.item_groups_key
where 
  c.product_group_description = 'Others - Spares'
  and a.status = 'invoiced'
  and a.invoiced_quantity <> 0
group by
  b.item_id,
  b.description
)

select
  a.product_id,
  a.product,
  a.product_sold_quantity,
  round(a.product_sold_value,2) as product_sold_value,
  a.product_last_sold_date,
  a.product_group,
  a.item_group,
  a.product_family,
  c.spare_id,
  c.spare,
  c.spare_sold_quantity,
  round(c.spare_sold_value,2) as spare_sold_value,
  c.spare_last_sold_date
from products a
left join powerbi.model.dim_flatbom b on a.product_id = b.product
left join spares c on b.child_item = c.spare_id
where
c.spare_sold_quantity <> 0
order by
  c.spare,
  c.spare_last_sold_date desc

 

 

/* Creation of a view for volume forecast variance report - wrangling data to allow for easier analysis within Power BI  */

CREATE VIEW [model].[fact_customer_volume_forecast] AS

with dates AS (
select
  fiscal_year_period,
  max(date_key) AS max_date_key
from powerbi.model.dim_date
group by fiscal_year_period
),
  
customer_sales AS (
select
  a.item_key,
  b.customer_reporting_group,
  d.max_date_key,
  sum(invoiced_quantity) AS gross_sales_volume
from powerbi.model.fact_sales_order_lines a
left join powerbi.model.dim_customers b ON a.invoice_to_customer_key = b.customers_key
left join powerbi.model.dim_date c ON a.invoice_date_key = c.date_key
left join dates d ON c.fiscal_year_period = d.fiscal_year_period
where a.sales_order_class = 'Sale'
group by
  a.item_key,
  b.customer_reporting_group,
  d.max_date_key
)

select
  b.max_date_key as forecast_generation_date_key,
  c.max_date_key as forecast_date_key,
  e.sector_description as sector,
  e.customers_key as customer_key,
  d.item_key as product_key,
  a.Period Reference as forecast_period_reference,
  max(a.[Reason for Manual Override]) as override_reason,
  max(a.[Calculated Forecast Method]) as calculated_forecast_method,
  sum(case when a.[Forecast Type] = 'Calculated Forecast' then cast(a.Forecast as float) end) as calculated_forecast,
  isnull(sum(case when a.[Forecast Type] = 'Manual Forecast' then cast(a.Forecast as float) end),0) as manual_forecast,
  sum(case when a.[Forecast Type] = 'Final Forecast' then cast(a.Forecast as float) end) as final_forecast,
  ISNULL(f.gross_sales_volume, 0) as gross_sold_volume
from powerbi.staging.combined_forecast a
left join dates b ON a.[Forecast Generation Period] = b.fiscal_year_period
left join dates c ON a.Forecast_Period = c.fiscal_year_period
left join powerbi.model.dim_item d ON a.[Product Code] = d.item_id
left join powerbi.model.dim_customers e ON a.[Customer] = e.customer_id
left join customer_sales f ON c.max_date_key = f.max_date_key and e.customer_reporting_group = f.customer_reporting_group and d.item_key = f.item_key
where
  e.sector_description in ('Retail','Online')
  and a.[Forecast Generation Period] >= '2024/07'
  and c.fiscal_year_period < (select fiscal_year_period from powerbi.model.dim_date where date_key = replace(CONVERT(char(10),getdate(),126),'-',''))
group by
  b.max_date_key,
  c.max_date_key,
  d.item_key,
  e.customers_key,
  e.sector_description,
  a.[Period Reference],
  ISNULL(f.gross_sales_volume, 0)
GO
