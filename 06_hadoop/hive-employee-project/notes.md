🌟 Mini Project: Querying Incremental CSV Data Using Hive on Hadoop
✔ Upload CSV datasets into HDFS in multiple batches (10 rows, then 20 rows)
✔ Organize data storage using HDFS directories
✔ Create an External Hive table on top of HDFS data
✔ Understand how Hive reads data without moving it
✔ Run Hive queries to validate incremental data ingestion
✔ Analyze how new files automatically reflect in Hive results
✔ Verify schema consistency and query accuracy


### What is the average salary of all employees?
hive> select avg(salary) from employee_data;
OK
89077.31818181818
Time taken: 18.344 seconds, Fetched: 1 row(s)
hive> 

### How many  employees (by first name = thomas)?
select count(*) from employee_data where first_name = "Thomas";
OK
2
Time taken: 15.884 seconds, Fetched: 1 row(s)
hive> 

### What is the average salary per team?
select avg(salary),team from employee_data group by team;

93326.5	
101792.25	Business Development
94382.0	Client Services
69819.66666666667	Distribution
87509.6	Engineering
84902.11111111111	Finance
81598.4	Human Resources
101033.16666666667	Legal
71145.2	Marketing
80404.14285714286	Product
100941.75	Sales
Time taken: 15.148 seconds, Fetched: 11 row(s)