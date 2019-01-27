import findspark
findspark.init()

from pyspark.sql import SparkSession
from pyspark.sql.functions import *

if __name__ == "__main__":
    spark = SparkSession.builder.appName("BuildIndex").getOrCreate()
    removePath = udf(lambda x: x.rsplit('/', 1)[-1])
    data = spark.read.text("dataset/*").withColumn("docId", input_file_name())
    data = data.withColumn("docId", removePath("docId")).sort(col("docId").asc())
    data = data.select(explode(split(data.value, "\s+")).alias("word"), 'docId')
    dictionary = spark.read.parquet("dictionary.parquet").sort(col("wordId").asc())
    joined = data.join(dictionary, data['word'] == dictionary['word'], 'inner')
    master = joined.select(col('wordId'), col('docId')).distinct().sort(col("wordId").asc())
    invertedIndex = master.groupBy("wordId").agg(collect_list("docId").alias("docId"))
    invertedIndex.write.save("index.parquet", format="parquet")

    spark.stop()
