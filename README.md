
This is an ETL pipeline where the python scripts are hosted on AWS Lambda, I used Amazon RDS (Postgres) to store the data.

The Data source is a calls metrics Api in which the calls table has 78 columns 

SqlAlchemy was used to write the data to the Postgres database

Make sure to add requests, sqlalchemy, pandas layers to your AWS Lambda function to enable you run this python code on Lambda