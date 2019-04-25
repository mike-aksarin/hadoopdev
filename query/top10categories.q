-- Select top 10 most frequently purchased categories

 select product_category
  from events
  group by product_category
  order by count(*)
  limit 10;
