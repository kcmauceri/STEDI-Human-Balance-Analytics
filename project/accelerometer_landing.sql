CREATE EXTERNAL TABLE IF NOT EXISTS stedi_data_lake.accelerometer_landing (
    user STRING,
    timestamp BIGINT,
    x DOUBLE,
    y DOUBLE,
    z DOUBLE
)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
WITH SERDEPROPERTIES (
    'case.insensitive' = 'false'
)
STORED AS INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat' 
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION 's3://stedi-data-lake-kam/accelerometer/landing/'
TBLPROPERTIES ('classification' = 'json');
