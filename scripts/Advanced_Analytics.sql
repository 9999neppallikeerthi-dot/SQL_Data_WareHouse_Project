--Change over time

select
datetrunc(month,order_date) as order_date,
sum(sales_amount) as total_sales,
count(distinct customer_key) as total_customers,
sum(quantity)as total_quantity
from gold.fact_sales
where order_date is not null
group by datetrunc(month,order_date)
order by datetrunc(month,order_date)

select * from gold.fact_sales
select * from gold.dim_products

-- cumulative analysis
-- calculate the total sales per month and the running total of sale over time.
select 
order_date,
total_sales,
sum(total_sales) over (order by order_date) as running_total_sales,
avg(avg_price) over (order by order_date) as moving_average_price
from
(
select 
datetrunc(year,order_date) as order_date,-- adding each row value to the sum of all the previous rows values and this is because of the default window frame b/w unbounded preceding and current row
sum(sales_amount) as total_sales,
avg(price) as avg_price
from gold.fact_sales
where order_date is not null
group by datetrunc(year,order_date) 

)t

-- performance analysis

/*Analyze the yearly performance of products by comparing each product's sales to both
its average sales performance and the previous years sales */

with yearly_product_sales as (
select
year(f.order_date) as order_year,
p.product_name,
sum(f.sales_amount) as current_sales
from gold.fact_sales f
left join gold.dim_products p
on f.product_key = p.product_key
where f.order_date is not null
group by year(f.order_date),p.product_name
)

select
order_year,
product_name,
current_sales,
avg(current_sales) over(partition by product_name)as avg_sales,
current_sales-avg(current_sales) over(partition by product_name) as diff_avg,
case when current_sales-avg(current_sales) over(partition by product_name) >0 then 'above avg'
	 when current_sales-avg(current_sales) over(partition by product_name)<0 then 'below avg'
	 else 'avg'
end avg_change,
lag(current_sales) over (partition by product_name order by order_year) previous_year_sales,
current_sales-lag(current_sales) over (partition by product_name order by order_year) previous_year_diff,
case when current_sales-lag(current_sales) over (partition by product_name order by order_year) >0 then 'Increase'
	 when current_sales-lag(current_sales) over (partition by product_name order by order_year)<0 then 'Decrease'
	 else 'No change'
end previous_year_change
from yearly_product_sales
order by product_name,order_year

-- part to whole analysis
/*which categories contribute the most to overall sales*/

with category_sales as (
select 
category,
sum(sales_amount) total_sales
from gold.fact_sales f
left join gold.dim_products p
on p.product_key=f.product_key
group by category)

select 
category,
total_sales,
sum(total_sales) over() overall_sales,
concat(round((cast(total_sales as float)/sum(total_sales) over())*100,2),'%') as percentage_of_total
from category_sales
order by total_sales desc

-- Data Segmentation
/*segment products into cost ranges and count how many products fall into each segment*/

with product_segments as(

select 
product_key,
product_name,
cost,
case when cost<100 then 'below 100'
	 when cost between 100 and 500 then '100-500'
	 when cost between 500 and 1000 then '500-1000'
	 else 'above 1000'
end cost_range
from gold.dim_products
)

select 
cost_range,
count(product_key) as total_products
from product_segments
group by cost_range
order by total_products desc


 
