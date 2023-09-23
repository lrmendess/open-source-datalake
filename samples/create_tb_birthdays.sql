CREATE SCHEMA IF NOT EXISTS hive.raw
WITH (location='s3a://samples/');

CREATE TABLE hive.raw.tb_birthdays (
    "id" int,
    "name" varchar,
    "birthday" varchar
) 
WITH (
    format='TEXTFILE',
    skip_header_line_count=1,
    textfile_field_separator=',',
    external_location='s3a://samples/tb_birthdays/'
);

SELECT * FROM hive.raw.tb_birthdays;
