ls 
mkdir data
cd data
wget https://github.com/shashank-mishra219/Hive-Class/blob/main/sales_order_data.csv
hdfs dfs -mkdir data
hdfs dfs -put /home/cloudera/data/sales_order_data.csv /user/cloudera/data
hive
create database hiveassigment1;
create table sales_order_csv
(
ORDERNUMBER string,
QUANTITYORDERED string,
PRICEEACH string,
ORDERLINENUMBER string,
SALES string,
STATUS string,
QTR_ID string,
MONTH_ID string,
YEAR_ID string,
PRODUCTLINE string,
MSRP string,
PRODUCTCODE string,
PHONE string,
CITY string,
STATE string,
POSTALCODE string,
COUNTRY string,
TERRITORY string,
CONTACTLASTNAME string,
CONTACTFIRSTNAME string,
DEALSIZE string
)
row format delimited
fields terminated by ','
tblproperties("skip.header.line.count"="1")
; 
Load data inpath "/user/cloudera/data/sales_order_data" into table sales_order_csv
create table sales_order_orc
(
ORDERNUMBER string,
QUANTITYORDERED string,
PRICEEACH string,
ORDERLINENUMBER string,
SALES string,
STATUS string,
QTR_ID string,
MONTH_ID string,
YEAR_ID string,
PRODUCTLINE string,
MSRP string,
PRODUCTCODE string,
PHONE string,
CITY string,
STATE string,
POSTALCODE string,
COUNTRY string,
TERRITORY string,
CONTACTLASTNAME string,
CONTACTFIRSTNAME string,
DEALSIZE string
)
stored as orc;
insert into sales_order_orc select * from sales_order_csv;
select year_id,sum(sales) from sales_order_orc group by year_id;
select PRODUCTLINE,count(ORDERNUMBER) as frequency from sales_order_orc  group by PRODUCTLINE order by frequency desc limit 1;
select QTR_ID,round(sum(sales)) as TotalSales from sales_order_orc group by QTR_ID;
create table quarter_sales as select QTR_ID,round(sum(sales),2) as TotalSales from sales_order_orc group by QTR_ID;
select QTR_ID,TotalSales from quarter_sales order by TotalSales asc limit 1;
create table country_sales as select COUNTRY,round(sum(sales),2) as totalSales from sales_order_orc group by COUNTRY ;
select * from country_sales order by totalSales desc limit 1;
select * from country_sales order by totalSales asc limit 1;
select s1.QTR_ID,country,TotalSales from sales_order_orc s1 join (select * from quarter_sales) s2 on s1.QTR_ID=s2.QTR_ID order by QTR_ID;
create table monthly_max as select YEAR_ID,max(quantityordered) as quantityordered1 from sales_order_orc group by YEAR_ID ;
select YEAR_ID,month_id,quantityordered1 from monthly_max s1 join (select month_id,quantityordered from sales_order_orc) s2 on s1.quantityordered1=s2.quantityordered ;
