from pyspark.sql.functions import col, explode
import os

from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.types import *

# Define PostgreSQL connection properties
conn_props = {
    "user": 'paramount',
    "password": 'password',
    "driver": "org.postgresql.Driver",
    "url": "jdbc:postgresql://localhost:5432/online_communities"
}


def extract_post_meta():
    post_meta_input_dir = "data/post_meta/"

    # define the schema for the Parquet files
    post_meta_schema = StructType([
        StructField("created_time", TimestampType(), True),
        StructField("type", StringType(), True),
        StructField("post_h_id", StringType(), True),
        StructField("page_h_id", StringType(), True),
        StructField("name_h_id", StringType(), True)
    ])

    # read the Parquet files into a PySpark DataFrame
    post_meta_df = spark.read \
        .schema(post_meta_schema) \
        .option("compression", "snappy") \
        .parquet(post_meta_input_dir)

    return post_meta_df


def transform_post_meta(post_meta_df):

    # perform some basic data cleaning and transformations
    post_meta_df = post_meta_df.filter(F.col("created_time").isNotNull())
    post_meta_df = post_meta_df.withColumn("type", F.trim(
        F.col("post_h_id"))).withColumnRenamed("type", "post_type")
    post_meta_df = post_meta_df.withColumn(
        "post_h_id", F.trim(F.col("post_h_id")))
    post_meta_df = post_meta_df.withColumn(
        "page_h_id", F.trim(F.col("page_h_id")))
    post_meta_df = post_meta_df.withColumn(
        "name_h_id", F.trim(F.col("name_h_id")))

    return post_meta_df


def load_post_meta_df(transformed):

    transformed.write \
        .format("jdbc") \
        .option("driver", conn_props['driver']) \
        .option("url", conn_props['url']) \
        .option("dbtable", "post_meta") \
        .option("user", conn_props['user']) \
        .option("password", conn_props['password']) \
        .mode("overwrite") \
        .save()


def extract_comment_text():

    comment_text_input_dir = 'data/comment_text/'

    comment_text_csv_files = [os.path.join(
        comment_text_input_dir, f) for f in os.listdir(comment_text_input_dir) if f.endswith('.csv')]

    # read the CSV file into a DataFrame
    comment_text_df = spark.read.csv(
        comment_text_csv_files, sep=',', header=True)
    return comment_text_df


def transform_comment_text(comment_text_df):

    # Removes null utf code so that it can be ingested into PSQL table
    comment_text_df = comment_text_df.filter(F.col("message").isNotNull())
    null = u'\u0000'
    return comment_text_df.withColumn('message', F.regexp_replace(comment_text_df['message'], null, ''))


def extract_comment_info():

    comment_info_input_dir = 'data/comment_info_jsonl/'

    # Define the schema
    comment_info_schema = StructType([
        StructField("h_id", StringType(), True),
        StructField("posts", StructType([
            StructField("data", ArrayType(StructType([
                StructField("comments", StructType([
                    StructField("data", ArrayType(StructType([
                        StructField("comment_count", LongType(), True),
                        StructField("comments", StructType([
                            StructField("data", ArrayType(StructType([
                                StructField("comment_count", LongType(), True),
                                StructField("created_time",
                                            StringType(), True),
                                StructField("h_id", StringType(), True),
                                StructField("parent", StructType([
                                    StructField("h_id", StringType(), True)
                                ])),
                                StructField("up_likes", LongType(), True)
                            ])), True)
                        ]), True),
                        StructField("created_time", StringType(), True),
                        StructField("h_id", StringType(), True),
                        StructField("up_likes", LongType(), True)
                    ])), True)
                ]), True),
                StructField("h_id", StringType(), True)
            ])), True)
        ]))
    ])

    comment_info_df = spark.read.schema(comment_info_schema).json(
        comment_info_input_dir, multiLine=True)
    return comment_info_df


def transform_comment_info(input_df):

    input_df.select(F.col('h_id').alias('page_h_id'),
                    "posts.data.h_id")

    input_df = input_df.select(F.col('h_id').alias('page_h_id'),
                               F.explode("posts.data").alias("posts"))

    # Explode the top level comments
    top_level_comments = input_df.select("page_h_id", F.col('posts.h_id').alias('post_h_id'), F.explode("posts.comments.data").alias("comments")) \
        .select("page_h_id", "post_h_id", "comments.h_id", "comments.created_time", "comments.up_likes") \
        .withColumn("top_level_comment_h_id", F.col('h_id')) \
        .withColumn('comment_type', F.lit('top_level'))

    input_df = input_df.select("page_h_id", F.col('posts.h_id').alias('post_h_id'),  F.explode(
        "posts.comments.data").alias("comments"))
    # Explode the replies
    reply_comments = input_df.select("page_h_id", 'post_h_id', F.explode(
        "comments.comments.data").alias("reply")).select(
        "page_h_id", "post_h_id", "reply.h_id", "reply.created_time", "reply.up_likes", F.col('reply.parent.h_id').alias('top_level_comment_h_id')).withColumn('comment_type', F.lit('reply'))

    # Union the top level and reply comments
    all_comments = top_level_comments.union(reply_comments)

    all_comments = all_comments.withColumn('created_time', F.to_date(
        F.regexp_replace(F.col('created_time'), 'T', ' ')))

    return all_comments


def load_table(df, table):
    df.write \
        .format("jdbc") \
        .option("driver", conn_props['driver']) \
        .option("url", conn_props['url']) \
        .option("dbtable", table) \
        .option("user", conn_props['user']) \
        .option("password", conn_props['password']) \
        .mode("overwrite") \
        .save()


if __name__ == "__main__":

    # create a SparkSession
    spark = SparkSession.builder \
        .appName("Paramount-Technical-Assessment") \
        .config("spark.jars.packages", "org.postgresql:postgresql:42.2.23") \
        .getOrCreate()

    post_meta_df = extract_post_meta()
    post_meta_df = transform_post_meta(post_meta_df)
    load_table(post_meta_df, "post_meta")

    # comment info
    comment_info_df = extract_comment_info()
    comment_info_df = transform_comment_info(comment_info_df)
    load_table(comment_info_df, "comment_info")

    # comment text
    comment_text_df = extract_comment_text()
    comment_text_df = transform_comment_text(comment_text_df)
    load_table(comment_text_df, "comment_text")

    spark.stop()
