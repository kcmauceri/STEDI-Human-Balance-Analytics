CREATE EXTERNAL TABLE `stedi_data_lake`.`step_trainer_landing` (
  `sensorReadingTime` BIGINT,
  `serialNumber` STRING,
  `distanceFromObject` INT
)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
WITH SERDEPROPERTIES (
  'ignore.malformed.json' = 'TRUE', 
  'dots.in.keys' = 'FALSE', 
  'case.insensitive' = 'TRUE'
)
STORED AS INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat' 
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION 's3://stedi-data-lake-kam/step_trainer/landing/'
TBLPROPERTIES ('classification' = 'json');
