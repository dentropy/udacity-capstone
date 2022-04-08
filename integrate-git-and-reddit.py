import pyspark
import pyspark.sql.functions as F
import pyspark.sql.types as T
import modules.parsers as parsers
import glob

conf = pyspark.SparkConf()
sc = pyspark.SparkContext.getOrCreate(conf=conf)
sqlcontext = pyspark.SQLContext(sc)

git_repo_df = sqlcontext.read.json('/home/paul/Projects/DataEngineering/Capstone/out/JOB_OUT')
git_domains_grouped = git_repo_df.groupBy("email_domain").count()
git_domains_grouped = git_domains_grouped.withColumnRenamed("count", "git_domain_count")

reddit_df = sqlcontext.read.json('/home/paul/Projects/DataEngineering/Capstone/out/JOB_OUT2')
reddit_domains_grouped = reddit_df.groupBy("domain_name").count()
reddit_domains_grouped = reddit_domains_grouped.withColumnRenamed("count", "reddit_domain_count")

unioned_domains = reddit_domains_grouped.unionByName(git_domains_grouped.withColumnRenamed("email_domain", "domain_name"),  allowMissingColumns=True)

unioned_domains.write.json("/home/paul/Projects/DataEngineering/Capstone/out/JOB_OUT3")