import findspark
findspark.init()

from pyspark.sql import SparkSession
from pyspark.sql.functions import *

if __name__ == "__main__":
    spark = SparkSession.builder.appName("BuildDictionary").getOrCreate()
    data = spark.read.text("./dataset/*")
    words = data.select(explode(split(data.value, "\s+")).alias("word")).distinct()
    words = words.withColumn("wordId", monotonically_increasing_id())
    dictionary = words.where(trim(words.word) != "").sort(col("wordId").asc())
    dictionary.write.save("dictionary.parquet", format="parquet")
    
    spark.stop()
