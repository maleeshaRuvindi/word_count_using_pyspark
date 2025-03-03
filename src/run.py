import argparse
import yaml
from datasets import load_dataset
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, expr, size, split, explode, lower, regexp_replace, count
from pyspark.sql.types import *

def load_config(config_path):
    """Load YAML configuration file."""
    with open(config_path, "r") as file:
        return yaml.safe_load(file)

def process_data(config):
    
    # Load dataset
    dataset = load_dataset(config["dataset_url"], split="test")
    df = pd.DataFrame(dataset)
    
    # Create PySpark session
    spark = SparkSession.builder.appName("AGNewsWordCount").getOrCreate()
    spark_df = spark.createDataFrame(df)
    spark_df.show(5)
    # Words to be searched
    target_words = config["words_to_count"]
    
    # Create empty dataframe for storing word counts
    emp_RDD = spark.sparkContext.emptyRDD()
    columns = StructType([
        StructField('word', StringType(), True),
        StructField('word_count', IntegerType(), True)
    ])
    word_counts_df = spark.createDataFrame(data=emp_RDD, schema=columns)
    
    for word in target_words:
        count_df = spark_df.withColumn(
            "word_count", size(split(col("description"), rf"(^|[^a-zA-Z]){word}([^a-zA-Z]|$)")) - 1
        ).selectExpr(f"'{word}' as word", "sum(word_count) as word_count")
        word_counts_df = word_counts_df.union(count_df)
    
    word_counts_df.show(5)
    
    # Save results
    output_path = config["output_path"] + "word_count.parquet"
    word_counts_df.write.parquet(output_path, mode="overwrite")

def process_data_all(config):
    
    # Load dataset
    dataset = load_dataset(config["dataset_url"], split="test")
    df = pd.DataFrame(dataset)
    
    
    # Create PySpark session
    spark = SparkSession.builder.appName("AGNewsWordCountAll").getOrCreate()
    spark_df = spark.createDataFrame(df)
    spark_df.show(5)
    
    # Remove special characters and tokenize words
    df_cleaned = spark_df.withColumn("description", lower(regexp_replace(col("description"), "[^a-zA-Z0-9\\s]", "")))
    df_words = df_cleaned.withColumn("word", explode(split(col("description"), "\\s+")))
    
    # Count occurrences of each word
    unique_word_count_df = df_words.groupBy("word").agg(count("*").alias("count"))
    
    # Save results
    output_path = config["output_path"] + "word_count_all.parquet"
    unique_word_count_df.write.parquet(output_path, mode="overwrite")

def main():
    parser = argparse.ArgumentParser(description="Process AG News dataset")
    parser.add_argument("command", choices=["process_data", "process_data_all"], help="Command to execute")
    parser.add_argument("--cfg", type=str, required=True, help="Path to config file")
    args = parser.parse_args()
    
    # Load configuration
    config = load_config(args.cfg)
    
    # Execute appropriate function
    if args.command == "process_data":
        process_data(config)
    elif args.command == "process_data_all":
        process_data_all(config)

if __name__ == "__main__":
    main()
