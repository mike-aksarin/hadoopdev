-- Select top 10 most frequently purchased categories

 select product_category, count(*) cnt
  from events
  group by product_category
  sort by cnt desc
  limit 10;
