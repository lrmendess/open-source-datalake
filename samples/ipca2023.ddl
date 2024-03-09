CREATE SCHEMA hive.bronze WITH (location='s3a://datalake-bronze/');

CREATE TABLE hive.bronze.ipca (
    "data" VARCHAR,
    "variacao" VARCHAR,
    "acumulado_12_meses" VARCHAR
) WITH (
    csv_escape='\',
    csv_quote='"',
    csv_separator='	',
    skip_header_line_count=1,
    external_location='s3a://datalake-landing/ipca/',
    format='csv'
);
