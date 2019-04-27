create table country_blocks_ipv4
(
  network string,
  geoname_id int,
  registered_country_geoname_id int,
  represented_country_geoname_id int,
  is_anonymous_proxy tinyint,
  is_satellite_provider tinyint
);

LOAD DATA LOCAL INPATH './csv/GeoLite2-Country-Blocks-IPv4.csv' OVERWRITE INTO TABLE country_blocks_ipv4;