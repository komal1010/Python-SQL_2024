#!pip install kaggle
import kaggle
!kaggle datasets download ankitbansal06/retail-orders -f orders.csv

#Extract Zip File
import zipfile

# Path to the zip file
zip_file_path = r'C:\Users\poshik\orders.csv.zip'

# Destination directory where files will be extracted
extract_to_path = r'C:\Users\poshik'

# Open the zip file in read mode
with zipfile.ZipFile(zip_file_path, 'r') as zip_ref:
    # Extract all the contents of the zip file to the specified directory
    zip_ref.extractall(extract_to_path)

  #Read data from the file and handle the null values
import pandas as pd
df = pd.read_csv('orders.csv',na_values=['Not Available', 'unknown'])
df.head(10)
#df['Ship Mode'].unique()

#rename the column name,make it lowercase and put the underscore 
#df.rename(columns = {'Order Id : order_id','Order Date : order_date','Ship Mode : ship_mode','Segment:segment','Country :country','City:city','State:state',
                    #'Postal Code:postal_code','Region :region','Category:category','Sub Category:sub_category','Product Id :product_id','cost price:cost_price',
                     #'List Price:list_price','Quantity:quantity','Discount Percent:discount_percent'})
df.columns = df.columns.str.lower()
df.columns= df.columns.str.replace(' ', '_')
df.head(10)

#derive new columns ,discounts, sale price and profit
df['discount']=df['list_price']*df['discount_percent']*.01
df['sale_price'] = df['list_price']-df['discount_percent']
df['profit']=df['sale_price']-df['cost_price']
df.head(10)

#convert orderdate from object datatype to datetime
df.dtypes
df['order_date'] = pd.to_datetime(df['order_date'],format="%Y-%m-%d")

#drop cost price, list price and discount and percent columns
df.drop(columns =['list_price','cost_price','discount_percent'],inplace=True)

#load this data to SQL server using replace option
import pandas as pd
import sqlalchemy as sal
engine = sal.create_engine('mssql://GDN-1800597816/master?driver=ODBC+DRIVER+17+FOR+SQL+SERVER')
conn=engine.connect()

#Load the data into sql server using append option
df.to_sql('df.orders',con=conn,index=False,if_exists='append')



------------------SQL code--------------------------------------------------------------------



select *from df_orders
where ship_mode IS NULL

update df_orders
set ship_mode = NULL where ship_mode ='unknown';

update df_orders
set ship_mode = '' where ship_mode IS NULL

select  ship_mode as Number, count(ship_mode) as Count from df_orders
group by ship_mode

create view Komal_Test.category as select * from df_orders where category= 'Furniture'

select * from category

with count_cte as (select * from df_orders where category= 'Furniture')
select * from count_cte where sub_category = 'Chairs'


select * from df_orders

drop table df_orders

create table df_orders([order_id] int primary key,
					[order_date] date, 
					[ship_mode] varchar(20),
					[segment] varchar(20),
					[country] varchar(20),
					[city] varchar(20),
					[state] varchar(20),
					[postal_code] varchar(20),
					[region] varchar(20),
					[category] varchar(20),
					[sub_category] varchar(20),
					[product_id] varchar(20),
					[quantity] varchar(20),
					[discount] decimal(7,2),
					[sale_price] decimal(7,2),
					[profit] decimal(7,2))
 
 select * from df_orders

 -- find top 10 highest revenue generating products
 select top 10 product_id, sum(sale_price) as sales
 from df_orders
 group by product_id
 order by sales desc

 -- find top 5 highest selling products in each region
 --select distinct region from df_orders
 with cte as (
	select region, product_id, sum(sale_price) as sales
	from df_orders
	 group by region,product_id)
	select * from(
	select *
	, row_number() over(partition by region order by sales desc)as rn
	from cte ) A
	where rn<=5
 --order by region,sales desc

 -- Find month over month growth comparision for 2022 and 2023 sales eg. jan 2022 vs jan 2023
 
with cte as (
	select year(order_date) as order_year,month(order_date) as order_month,
	sum(sale_price) as sales
	from df_orders
	group by year(order_date),month(order_date)
	--order by year(order_date),month(order_date)
	)
	select order_month
	, sum(case when order_year=2022 then sales else 0 end ) as sales_2022
	, sum(case when order_year=2023 then sales else 0 end ) as sales_2023
	from cte 
	group by order_month
	order by order_month

--for each category wich month had highest sales
with cte as ( 
select category, format(order_date,'yyyyMM')as order_year_month
, sum(sale_price) as sales
from df_orders 
group by category, format(order_date,'yyyyMM')
--order by category, format(order_date,'yyyyMM')
)
select * from(
select *
, row_number() over(partition by category order by sales desc) as rn
from cte
) a
where rn=1

--which sub category had highest growth profit in 2023 compare to 2022

with cte as (
	select sub_category, year(order_date) as order_year,
	sum(sale_price) as sales
	from df_orders
	group by sub_category, year(order_date)
	--order by year(order_date),month(order_date)
	)
	,cte2 as(
	select sub_category
	, sum(case when order_year=2022 then sales else 0 end ) as sales_2022
	, sum(case when order_year=2023 then sales else 0 end ) as sales_2023
	from cte 
	group by sub_category)
	select top 1*
	,(sales_2023-sales_2022)*100/sales_2022
	from cte2
	order by (sales_2023-sales_2022)*100/sales_2022 desc
	--order by order_month

