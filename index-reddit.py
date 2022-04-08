import pyspark
import modules.parsers as parsers

conf = pyspark.SparkConf()
sc = pyspark.SparkContext.getOrCreate(conf=conf)
sqlcontext = pyspark.SQLContext(sc)



reddit_df = sqlcontext.read.json('/home/paul/Projects/DataEngineering/Capstone/data/001.ndjson')# RC_2018-01-01.ndjson')
reddit_df = parsers.df_extract_url(reddit_df, "body")
# reddit_df = parsers.df_parse_domainname(reddit_df, "url")

reddit_df.write.json("/home/paul/Projects/DataEngineering/Capstone/out/JOB_OUT2")
