CREATE SCHEMA hive.raw WITH (location='s3a://datalake-raw/');
CREATE SCHEMA hive.trusted WITH (location='s3a://datalake-trusted/');
CREATE SCHEMA hive.refined WITH (location='s3a://datalake-refined/');