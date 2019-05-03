select country_name, sum(product_price) cost
from events
-- to be replaced with user-defined function
join country_blocks_ipv4 on substring(client_ip, 1, 9) = substring(network, 1, 9)
join country_locations on country_blocks_ipv4.geoname_id = country_locations.geoname_id
group by country_name
sort by cost desc
limit 10;