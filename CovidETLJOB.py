import sys
import redshift_connector
conn = redshift_connector.connect(
    host="redshift-cluster-1.c6hyahorsvry.ap-south-1.redshift.amazonaws.com",
    database="dev",
    user="awsuser",
    password=[]
)

conn.autocommit = True

cursor = redshift_connector.Cursor = conn.cursor()

### Create tables in redshift ###

cursor.execute("""
CREATE TABLE "dimDate" (
"index" INTEGER,
  "fips" REAL,
  "date" TIMESTAMP,
  "year" INTEGER,
  "month" INTEGER,
  "day_of_week" INTEGER
)
""")

cursor.execute("""
CREATE TABLE "dimHospital" (
"index" INTEGER,
  "fips" REAL,
  "state_name" TEXT,
  "latitude" REAL,
  "longtitude" REAL,
  "hq_address" TEXT,
  "hospital_name" TEXT,
  "hospital_type" TEXT,
  "hq_city" TEXT,
  "hq_state" TEXT
)
""")

cursor.execute("""
CREATE TABLE "factCovid" (
"index" INTEGER,
  "fips" REAL,
  "province_state" TEXT,
  "country_region" TEXT,
  "confirmed" REAL,
  "deaths" REAL,
  "recovered" REAL,
  "active" REAL,
  "date" INTEGER,
  "positive" INTEGER,
  "negative" REAL,
  "hospitalizedcurrently" REAL,
  "hospitalized" REAL,
  "hospitalizeddischarged" REAL
)
""")

cursor.execute("""
CREATE TABLE "dimRegion" (
"index" INTEGER,
  "fips" REAL,
  "province_state" TEXT,
  "country_region" TEXT,
  "latitude" REAL,
  "longitude" REAL,
  "county" TEXT,
  "state" TEXT
)
""")

### Copy Data from S3 to Redshit ###

cursor.execute("""
copy dimDate from 's3://ath-covid-de-project/output/dimDate.csv'
credentials 'aws_access_key_id=AKIA5LPLVSFJOFGPZO6T;aws_secret_access_key=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'
delimiter ','
region 'ap-south-1'
IGNOREHEADER 1
""")

cursor.execute("""
copy dimHospital from 's3://ath-covid-de-project/output/dimHospital.csv'
credentials 'aws_access_key_id=AKIA5LPLVSFJOFGPZO6T;aws_secret_access_key=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'
delimiter ','
region 'ap-south-1'
IGNOREHEADER 1
""")

cursor.execute("""
copy factCovid from 's3://ath-covid-de-project/output/factCovid.csv'
credentials 'aws_access_key_id=AKIA5LPLVSFJOFGPZO6T;aws_secret_access_key=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'
delimiter ','
region 'ap-south-1'
IGNOREHEADER 1
MAXERROR 10
STATUPDATE ON
""")

cursor.execute("""
copy dimRegion from 's3://ath-covid-de-project/output/dimRegion.csv'
credentials 'aws_access_key_id=AKIA5LPLVSFJOFGPZO6T;aws_secret_access_key=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'
delimiter ','
region 'ap-south-1'
IGNOREHEADER 1
MAXERROR 10
STATUPDATE ON
""")
