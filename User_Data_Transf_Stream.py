from pyspark.streaming import StreamingContext
from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, trim, to_json, struct, from_json, concat, lit
from pyspark.sql.types import IntegerType

#Spark base
sc = SparkContext()
spark = SparkSession(sc)
ssc = StreamingContext(sc,1)

#Kafka input stream
input_df = spark\
  .readStream\
  .format("kafka")\
  .option("kafka.bootstrap.servers", "localhost:9092")\
  .option("subscribe", "input")\
  .option("startingOffsets", "earliest") \
  .option("failOnDataLoss", "false") \
  .load()

#getting data from input stream

interval = input_df.select(col("value").cast("string")).alias("csv").select("csv.*")

#splitting data from csv format to table format

data = interval\
    .selectExpr("split(value,',')[0] as id"\
    ,"split(value,',')[1] as name"\
    ,"split(value,',')[2] as country"\
    ,"split(value,',')[3] as age")

data = data.select(col("id"), col("name"), col("country"), trim(col("age")).alias("age"))

#cast to ing
data = data.withColumn("age", data["age"].cast(IntegerType()))

#filter
data = data.filter(data["age"] > 30)

#transform to json
json_data = data.select(to_json(struct(col("id"), col("name"), col("age"))).alias("value"))

# json_data = data.select(to_json(struct(col("*"))).alias("value"))

'''
#json to struct
data = data.withColumn('json', from_json(col('json'), 'struct<id:STRING, name:STRING, country:STRING, age:INT>'))

#from stuct to country
final_data = data.select(concat(col("json.id"),  lit(','), col("json.name"), lit(','), col("json.age")).alias("value"))
'''

#Streaming to output topic
stream_to_kafka = json_data\
    .writeStream\
    .format("kafka")\
    .option("kafka.bootstrap.servers", "localhost:9092")\
    .option("topic", "output")\
    .option("checkpointLocation", "/home/gyurkovics/")\
    .start()

#Kafka output stream
output_df = spark\
  .readStream\
  .format("kafka")\
  .option("kafka.bootstrap.servers", "localhost:9092")\
  .option("subscribe", "output")\
  .option("startingOffsets", "earliest") \
  .option("failOnDataLoss", "false") \
  .load()

#reading output stream
output_interval = output_df.select(col("value").cast("string")).alias("json_data").select("json_data.*")

#Showing on console
stream_to_console = output_interval\
    .writeStream\
    .format("console")\
    .start()

#keeping open the streams
stream_to_kafka.awaitTermination()
stream_to_console.awaitTermination()
