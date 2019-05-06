-- Select top 10 most frequently purchased product in each category

select product_name, count(*) cnt
  from events
  group by product_name
  order by cnt desc
  limit 10;