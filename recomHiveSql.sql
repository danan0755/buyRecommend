--hive数据库
create database if not exists recomm;
--测试表：保存推荐模型计算产生的少量数据
create  table  if not exists result(
   skuId int,
   sallId int,
   cosim double
)stored as orc tblproperties("orc.compress"="SNAPPY"); 

--查询有效数据，过滤完全一样的sku
select * from result where cosim <0.99999 sort by cosim  desc limit 9 ;

--生产表：保存推荐模型计算产生的大量数据

create  table  if not exists similar(
   skuId int,
   sallId int,
   cosim double
)stored as orc tblproperties("orc.compress"="SNAPPY"); 




