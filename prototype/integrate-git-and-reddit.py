import pyspark
import pyspark.sql.functions as F
import pyspark.sql.types as T
import modules.parsers as parsers
import glob

import argparse

parser = argparse.ArgumentParser()
parser.add_argument("--gitin",   help="Path for git JSON directory")
parser.add_argument("--redditin",  help="Path for reddit JSON directory")
parser.add_argument("--outpath", help="Directory to save the output.")
args = parser.parse_args()
if args.gitin:
    gitin = args.gitin
elif args.redditin:
    gitin = args.redditin
elif args.outpath:
    outpath = args.outpath
else:
    raise Exception('Missing some arguments')

conf = pyspark.SparkConf()
sc = pyspark.SparkContext.getOrCreate(conf=conf)
sqlcontext = pyspark.SQLContext(sc)

git_repo_df = sqlcontext.read.json(args.gitin)
git_domains_grouped = git_repo_df.groupBy("email_domain").count()
git_domains_grouped = git_domains_grouped.withColumnRenamed("count", "git_domain_count")

reddit_df = sqlcontext.read.json(args.redditin)
reddit_domains_grouped = reddit_df.groupBy("domain_name").count()
reddit_domains_grouped = reddit_domains_grouped.withColumnRenamed("count", "reddit_domain_count")

unioned_domains = reddit_domains_grouped.unionByName(git_domains_grouped.withColumnRenamed("email_domain", "domain_name"),  allowMissingColumns=True)

unioned_domains.write.json( args.outpath )