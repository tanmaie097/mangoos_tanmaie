import os

# Go to ETL project folder
os.chdir("/Users/tanmaie/Desktop/magnoos/exercises/04_ETL/news_etl")

# Run extract
os.system("/Users/tanmaie/Desktop/magnoos/exercises/magnoos_env/bin/python3 extract.py")

# Run transform (CSV creation)
os.system("/Users/tanmaie/Desktop/magnoos/exercises/magnoos_env/bin/python3 transform.py")

print("ETL Completed Successfully!")
