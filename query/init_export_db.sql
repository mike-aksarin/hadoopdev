-- running from root user

create user hdev WITH PASSWORD 'hdev';

create database hdev;

grant all on database hdev to hdev;

\c hdev

create table top_categories
(
  product_category varchar primary key,
  purchase_count int
);

grant all on top_categories to hdev;

create table top_products
(
  product_name varchar primary key,
  purchase_count int
);

grant all on top_products to hdev;

create table top_countries
(
  country_name varchar primary key,
  spent_total bigint
);

grant all on top_countries to hdev;
