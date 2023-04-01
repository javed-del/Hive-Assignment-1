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
create table
