CREATE EXTERNAL TABLE stedi_data_lake.customer_curated (
  serialNumber STRING,
  shareWithPublicAsOfDate BIGINT,
  birthDay STRING,
  registrationDate BIGINT,
  shareWithResearchAsOfDate BIGINT,
  customerName STRING,
  shareWithFriendsAsOfDate BIGINT,
  email STRING,
  lastUpdateDate BIGINT,
  phone STRING
)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
WITH SERDEPROPERTIES (
  "serialization.format" = "1"
)
LOCATION 's3://stedi-data-lake-kam/customer/curated/'
TBLPROPERTIES ('has_encrypted_data'='false');
