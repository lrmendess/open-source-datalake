CREATE SCHEMA IF NOT EXISTS hive.landing
WITH (location='s3a://landing/');

CREATE TABLE hive.landing.tb_birthdays (
    "id" int,
    "name" varchar,
    "birthday" varchar
) 
WITH (
    format='TEXTFILE',
    skip_header_line_count=1,
    textfile_field_separator=',',
    external_location='s3a://landing/tb_birthdays/'
);

SELECT * FROM hive.landing.tb_birthdays;
