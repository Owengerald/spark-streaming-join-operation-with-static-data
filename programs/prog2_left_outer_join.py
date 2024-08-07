from pyspark.sql.functions import *
from pyspark.sql import SparkSession

if __name__ == '__main__':
    
    print("Creating Spark Session")

    spark = SparkSession.builder \
            .appName("streaming join application") \
            .config("spark.sql.shuffle.partitions", 3) \
            .master("local[2]") \
            .getOrCreate()

    # defining the transactions streaming data schema
    transactions_schema =  "card_id long, amount long, postcode long, pos_id long, transaction_dt timestamp"

    # 1. reading the data
    transactions_df = spark \
        .readStream \
        .format("socket") \
        .option("host", "localhost") \
        .option("port", "9979") \
        .load()

    # getting value dataframe by parsing json data in transaction dataframe
    value_df = transactions_df.select(from_json(col("value"), transactions_schema).alias("values"))

    # getting refined transactions dataframe by selecting actual columns in the value dataframe
    refined_transactions_df = value_df.select("value.*")

    #refined_orders_df.printSchema()

    # creating the members dataframe (static data) 
    members_df = spark.read \
        .format("csv") \
        .option("header", True) \
        .option("inferSchema", True) \
        .option("path", "../data/<member_table>") \
        .load()

    #members_df.show()

    # defining the join expression/condition
    join_expr = refined_transactions_df.card_id == members_df.card_id

    # defining the join type (left outer join)
    join_type = "leftouter"

    # creating the enriched dataframe by joining the streaming data and the static data
    enriched_df = refined_transactions_df.join(members_df, join_expr, join_type).drop(members_df["card_id"])

    # writing stream to output
    streaming_query = enriched_df \
        .writeStream \
        .format("console") \
        .outputMode("update") \
        .option("checkpointLocation", "checkpoint1") \
        .trigger(processingTime = "15 seconds") \
        .start()

    # making the streaming query await termination
    streaming_query.awaitTermination()