# import SQLContext and HiveCOntext

from pyspark.sql import functions as F
from pyspark.sql.types import StringType
from pyspark.sql.types import IntegerType
from pyspark import SQLContext
from pyspark.sql import HiveContext
from functools import reduce
from pyspark.sql import DataFrame
from pyspark.sql import Window



# read data file each, split the data by delimiter and return rdd with first 4 columns
allData = sc.textFile("file:////home/cloudera/Mukul/Noodle/python/2017.csv").map(lambda line: (line.split(',')[0], line.split(',')[1], line.split(',')[2], line.split(',')[3]))

# convert rdd inot DataFrame and assign name column name
df = allData.toDF(['stn_id','obs_dt', 'obs_type', 'obs_value'])

# cast observation value (obs_value) into integer to aviod string error in aggregate
df = df.withColumn("obs_value",df["obs_value"].cast(IntegerType()))

# filter DataFrame data only for the required observation type (obs_type)
df = df[df.obs_type.isin(['PRCP', 'TMAX', 'TMIN', 'SNOW', 'SNWD', 'EVAP', 'WESD ', 'WESF', 'PSUN'])]

# transform data (rows to column)
df1 = df.groupby('stn_id','obs_dt').pivot('obs_type').sum('obs_value')

# register DataFrame into a temp table so later it can be called for the Hive table 
df1.registerTempTable("temp_weathercurated_py")

# set hive context so hive table can be created from the spark session
hive_context = HiveContext(sc)

# create hive table
hive_context.sql("create table weathercurated_py as select * from temp_weathercurated_py")
