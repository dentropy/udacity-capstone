import pyspark
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import pyspark.sql.types as T
import modules.parsers as parsers
import configparser
import os
import sys

config = configparser.ConfigParser()
config.read('dl.cfg')
os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    spark = SparkSession.builder\
                         .config("spark.jars.packages","org.apache.hadoop:hadoop-aws:2.7.3")\
                         .master("spark://localhost:7077")\
                         .enableHiveSupport()\
                         .getOrCreate()
    return spark


def process_git_data(spark, git_data_url, output_url):
    df_git = spark.read.json(git_data_url)
    git_repo_df = parsers.df_parse_email(df_git,  "author_email")
    git_domains_grouped = git_repo_df.groupBy("email_domain").count()
    git_domains_grouped = git_domains_grouped.withColumnRenamed("count", "git_domain_count")
    git_domains_grouped.write.parquet(output_url, mode="overwrite")
    return git_domains_grouped

def process_reddit_data(spark, reddit_data_url, output_url):
    df_reddit = spark.read.json(reddit_data_url)
    reddit_df = parsers.df_extract_url(df_reddit, "body")
    reddit_df = parsers.df_parse_domainname(reddit_df, "url")
    reddit_domains_grouped = reddit_df.filter( F.col("url") != "" ).groupBy("domain_name").count()
    reddit_domains_grouped = reddit_domains_grouped.withColumnRenamed("count", "reddit_domain_count")
    reddit_domains_grouped.write.parquet(output_url, mode="overwrite")
    return reddit_domains_grouped

def join_dataframes(spark, git_data_url, reddit_data_url, output_path):
    reddit_domains_grouped = spark.read.parquet(reddit_data_url)
    git_domains_grouped = spark.read.parquet(git_data_url)
    # git_domains_grouped = git_domains_grouped.withColumnRenamed("email_domain", "domain_name")
    joined_domains = reddit_domains_grouped.alias("reddit")\
      .join(git_domains_grouped.alias("git"), F.col("git.email_domain") == F.col("reddit.domain_name"))
    joined_domains.write.parquet(output_path, mode="overwrite")


def main():
    spark = create_spark_session()
    
    bucket_url = "s3a://paul-udacity-capstone"
    
    git_data_path = "/git-dump/*/*.json"
    # git_data_path = "/git-dump/freeCodeCamp/freeCodeCamp.json" # For Testing
    git_data_url = bucket_url + git_data_path
    # git_domains_grouped = process_git_data(spark, git_data_url, bucket_url + "/git_domains_final.parquet")
    
    
    # File is 1.5 Gb will take a couple minutes to load into spark
    reddit_data_path = "/reddit/RC_2018_01_01"
    # reddit_data_path = "/reddit/002.ndjson" # For Testing
    reddit_data_url = bucket_url + reddit_data_path
    # reddit_domains_grouped = process_reddit_data(spark, reddit_data_url , bucket_url + "/reddit_domains_final.parquet")
    
    join_dataframes(spark, 
                    bucket_url + "/git_domains_final.parquet",
                    bucket_url + "/reddit_domains_final.parquet", 
                    bucket_url + "/joined_domains_final.parquet")

    print("All done")
    sys.exit()

if __name__ == "__main__":
    main()