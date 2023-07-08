CREATE EXTERNAL TABLE IF NOT EXISTS `project`.`step_trainer_landing` (
  `sensorreadingtime` bigint,
  `serialnumber` string,
  `distancefromobject` int,
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
LOCATION 's3://neidhpa-lakehouse-project/step_trainer/'
TBLPROPERTIES ('classification' = 'json');