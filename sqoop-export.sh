#!/usr/bin/env bash

sqoop --options-file conf/sqoop.param --table top_categories --hcatalog-table top_categories

sqoop --options-file conf/sqoop.param --table top_products --hcatalog-table top_products

sqoop --options-file conf/sqoop.param --table top_countries --hcatalog-table top_countries