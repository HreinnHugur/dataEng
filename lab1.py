import os
import string
import sys

from nltk import word_tokenize, ngrams
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode, size, udf
from pyspark.sql.types import ArrayType, StringType

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable


def clean_and_lowercase(message):
    message = message.translate(str.maketrans('', '', string.punctuation))
    message = message.lower()
    return message


def extract_3grams_from_text(text):
    tokens = word_tokenize(text)[:5]
    if len(tokens) < 5:
        return []
    three_grams = list(ngrams(tokens, 3))
    return [" ".join(gram) for gram in three_grams]


spark = SparkSession.builder.appName("GitHubAnalysis").getOrCreate()

jsonl_file_path = "10K.github.jsonl"
df = spark.read.json(jsonl_file_path)

filtered_push_events = df.filter((size(col("payload.commits")) > 0) & (col("type") == "PushEvent"))
exploded_commits = filtered_push_events.select("payload.commits").withColumn("commit_info", explode(col("commits")))

push_events = exploded_commits.select(
    "commit_info.author.name",
    "commit_info.message"
).withColumnRenamed("name", "author_name").withColumnRenamed("message", "commit_message")

clean_message_udf = udf(clean_and_lowercase, StringType())
generate_3grams_udf = udf(extract_3grams_from_text, ArrayType(StringType()))

push_events = push_events.withColumn("cleaned_message", clean_message_udf(col("commit_message")))
push_events = push_events.withColumn("3grams", generate_3grams_udf(col("cleaned_message")))

filtered_3grams = push_events.filter(size(col("3grams")) > 0)
selected_columns = ["author_name"] + [col("3grams")[i].alias(f"3gram{i+1}") for i in range(3)]
final_result = filtered_3grams.select(*selected_columns)

final_result.write.csv("output.csv", header=True, mode="overwrite")

spark.stop()
