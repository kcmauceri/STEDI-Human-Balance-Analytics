CREATE EXTERNAL TABLE IF NOT EXISTS stedi_data_lake.machine_learning_curated (
    user STRING,
    email STRING,
    serialNumber STRING,
    sensorReadingTime BIGINT,
    step_count INT,
    distanceFromObject DOUBLE,
    x DOUBLE,
    y DOUBLE,
    z DOUBLE
)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
WITH SERDEPROPERTIES (
    'ignore.malformed.json' = 'FALSE',
    'dots.in.keys' = 'FALSE',
    'case.insensitive' = 'TRUE',
    'mapping' = 'TRUE'
)
STORED AS INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat' 
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION 's3://stedi-data-lake-kam/machine_learning/curated/'
TBLPROPERTIES ('classification' = 'json');
