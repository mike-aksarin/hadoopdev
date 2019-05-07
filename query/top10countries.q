ADD JAR hadoopdev_2.11-0.1.jar;
ADD JAR scala-library-2.11.8.jar;
ADD JAR scala-reflect-2.11.8.jar;

select country_name, sum(product_price) cost
from events
-- assume that all network mask are greater than 7 bits
join country_blocks_ipv4 on cast(split(client_ip, '\\.')[0] as int) % 128 = cast(split(network, '\\.')[0] as int) % 128
join country_locations on country_blocks_ipv4.geoname_id = country_locations.geoname_id
where match_network(client_ip, network) -- call user-defined function
group by country_name
order by cost desc
limit 10;

-- whether any network mask is less than 7 bits
-- select * from country_blocks_ipv4 where split(network, "/")[1] < 7;