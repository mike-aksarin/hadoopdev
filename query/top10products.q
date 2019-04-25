-- Select top 10 most frequently purchased product in each category

select product_name
  from events
  group by product_name
  order by count(*)
  limit 10;