select country_name, sum(product_price) cost
from events
join country_blocks_ipv4 on match_network(client_ip, network)
join country_locations on country_blocks_ipv4.geoname_id = country_locations.geoname_id
group by country_name
order by cost desc
limit 10;