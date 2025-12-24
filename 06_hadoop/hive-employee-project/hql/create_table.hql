CREATE TEMPORARY TABLE employee_data (
  first_name STRING,
  gender STRING,
  start_date STRING,
  last_login_time STRING,
  salary INT,
  bonus_pct DOUBLE,
  senior_management STRING,
  team STRING
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
  "separatorChar" = ",",
  "quoteChar"     = "\"",
  "escapeChar"    = "\\"
)
STORED AS TEXTFILE
LOCATION '/user/tanmaie/hive_01_empData'
TBLPROPERTIES ("skip.header.line.count"="1");
