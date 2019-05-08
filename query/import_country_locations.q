create table country_locations
(
  geoname_id int,
  locale_code varchar(2),
  continent_code varchar(2),
  continent_name string,
  country_iso_code varchar(2),
  country_name string,
  is_in_european_union tinyint
)
row format delimited
    fields terminated by ','
    escaped by '\\'
    collection items terminated by '|'
    lines terminated by '\n'
  stored as textfile;

LOAD DATA LOCAL INPATH './csv/GeoLite2-Country-Locations-en.csv' OVERWRITE INTO TABLE country_locations;