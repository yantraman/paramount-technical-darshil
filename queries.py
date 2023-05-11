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


def read_table(table):

    # create a SparkSession
    spark = SparkSession.builder \
        .appName("Paramount-Technical-Assessment") \
        .config("spark.jars.packages", "org.postgresql:postgresql:42.2.23") \
        .getOrCreate()

    # Read data from PostgreSQL table
    return spark.read \
        .format("jdbc") \
        .option("driver", conn_props['driver']) \
        .option("url", conn_props['url']) \
        .option("dbtable", table) \
        .option("user", conn_props['user']) \
        .option("password", conn_props['password']) \
        .load()


if __name__ == "__main__":
    # main()
    # create a SparkSession
    spark = SparkSession.builder \
        .appName("Paramount-Technical-Assessment") \
        .config("spark.jars.packages", "org.postgresql:postgresql:42.2.23") \
        .getOrCreate()

    comment_info_df = read_table("comment_info")
    comment_text_df = read_table("comment_text")
    post_meta_df = read_table("post_meta")

    # 1. Find the date with the most top level comments
    comment_count = comment_info_df.groupby('created_time').agg(
        F.countDistinct("h_id").alias("comment_count")).sort(F.desc("comment_count"))
    comment_count.show(1)  # Shows the first date

    # 2. Report back the number of comments, and comment up_likes, per type, for all comments (top level and replies) made after 2018-01-10

    # Filter comments made after 2018-01-10
    filtered_comments = comment_info_df.filter(
        F.col("created_time") > "2018-01-10")

    # Group by comment type and calculate counts and sum of up_likes
    comment_stats = filtered_comments.groupBy("comment_type") \
        .agg(F.count("*").alias("comment_count"), F.sum("up_likes").alias("total_up_likes")).show(truncate=False)

    # For each page, find the average length per comment (number of characters). Include top level comments and replies.
    # What are the top 5 pages, sorted by highest average length of comment. Please provide page names, page ids, and values for average length of comment.
    average_lengths = comment_info_df.join(comment_text_df, ['h_id'], 'left').join(post_meta_df, ['page_h_id', 'post_h_id'], 'left').groupBy(
        "post_h_id", "page_h_id").agg(F.avg(F.length("message")).alias("average_length")).sort(F.desc('average_length'))
    average_lengths.show(5, truncate=False)
