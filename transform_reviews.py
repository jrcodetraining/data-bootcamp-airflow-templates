from pyspark.sql import SparkSession
from pyspark.sql.functions import array_contains, when
from pyspark.ml.feature import Tokenizer, StopWordsRemover


spark = SparkSession.builder.appName("ExportReviewswithPySpark").getOrCreate()

movies_reviews = spark.read.option("header","true").csv("gs://bucket/raw-layer/reviews.csv")

movies_tokenized = Tokenizer(inputCol="review_str", outputCol="words").transform(movies_reviews)

movies_cleaned = StopWordsRemover(inputCol="words", outputCol="review_token").transform(movies_tokenized)

movies_filtered = movies_cleaned

movies_positive = movies_filtered.select(movies_filtered.cid, array_contains(movies_filtered.review_token,"good").alias("positive"))
movies_positive_int = movies_positive.withColumn("positive_int", when(movies_positive.positive == "true", 1).otherwise(0))

movies_positive_final = movies_positive_int.select(movies_positive_int.cid, movies_positive_int.positive_int)

movies_positive_final.repartition(1).write.option("header","true").csv("gs://bucket/staging-layer/reviews")

