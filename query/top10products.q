-- Select top 10 most frequently purchased product in each category

-- these two lines are to prepare for the sqoop export
-- drop table if exists top_products;
-- create table top_products as

select product_name, count(*) purchase_count
  from events
  group by product_name
  order by purchase_count desc
  limit 10;