import pyspark
import modules.parsers as parsers

import argparse

parser = argparse.ArgumentParser()
parser.add_argument("--inpath",   help="Path for git JSON file")
parser.add_argument("--outpath",  help="Directory to save the output.")
args = parser.parse_args()
if args.inpath:
    globpath = args.inpath
elif args.outpath:
    outpath = args.outpath
else:
    raise Exception('Missing some arguments')

conf = pyspark.SparkConf()
sc = pyspark.SparkContext.getOrCreate(conf=conf)
sqlcontext = pyspark.SQLContext(sc)



reddit_df = sqlcontext.read.json(args.inpath)# RC_2018-01-01.ndjson')
reddit_df = parsers.df_extract_url(reddit_df, "body")
reddit_df = parsers.df_parse_domainname(reddit_df, "url")

reddit_df.write.json(args.outpath)
