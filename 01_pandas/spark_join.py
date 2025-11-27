import tempfile
import csv
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("csv_workaround").getOrCreate()

data1 = [(1, "Alice"), (2, "Bob"), (3, "Charlie")]
data2 = [(1, "HR"), (2, "Finance"), (4, "Marketing")]

# write temp CSVs
tmp1 = tempfile.NamedTemporaryFile(mode="w", delete=False, suffix=".csv")
tmp2 = tempfile.NamedTemporaryFile(mode="w", delete=False, suffix=".csv")

with open(tmp1.name, "w", newline="") as f:
    writer = csv.writer(f)
    writer.writerow(["id","name"])
    writer.writerows(data1)

with open(tmp2.name, "w", newline="") as f:
    writer = csv.writer(f)
    writer.writerow(["id","department"])
    writer.writerows(data2)

df1 = spark.read.csv(tmp1.name, header=True, inferSchema=True)
df2 = spark.read.csv(tmp2.name, header=True, inferSchema=True)

df1.show()
df2.show()

joined = df1.join(df2, on="id", how="right")
joined.show()

spark.stop()

