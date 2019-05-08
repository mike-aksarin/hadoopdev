-- Select top 10 most frequently purchased categories

-- these two lines are to prepare for the sqoop export
-- drop table if exists top_categories;
-- create table top_categories as

select product_category, count(*) purchase_count
  from events
  group by product_category
  order by purchase_count desc
  limit 10;
