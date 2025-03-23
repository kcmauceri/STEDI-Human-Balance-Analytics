CREATE EXTERNAL TABLE `stedi_data_lake`.`customer_landing` (
  `serialNumber` STRING,
  `shareWithPublicAsOfDate` BIGINT,
  `birthDay` STRING,
  `registrationDate` BIGINT,
  `shareWithResearchAsOfDate` BIGINT,
  `customerName` STRING,
  `email` STRING,
  `lastUpdateDate` BIGINT,
  `phone` STRING,
  `shareWithFriendsAsOfDate` BIGINT
)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
WITH SERDEPROPERTIES (
  'ignore.malformed.json' = 'TRUE', 
  'dots.in.keys' = 'FALSE', 
  'case.insensitive' = 'TRUE'
)
STORED AS INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat' 
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION 's3://stedi-data-lake-kam/customer/landing/'
TBLPROPERTIES ('classification' = 'json');
