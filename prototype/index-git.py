import pyspark
import pyspark.sql.functions as F
import pyspark.sql.types as T
import modules.parsers as parsers
import glob

import argparse

parser = argparse.ArgumentParser()
parser.add_argument("--globpath", help="Glob path for git JSON files.")
parser.add_argument("--outpath",  help="Directory to save the output.")
args = parser.parse_args()
if args.globpath:
    globpath = args.globpath
elif args.outpath:
    outpath = args.outpath
else:
    raise Exception('Missing some arguments')

print(args.outpath)
print(args.globpath)

conf = pyspark.SparkConf()
sc = pyspark.SparkContext.getOrCreate(conf=conf)
sqlcontext = pyspark.SQLContext(sc)

## Working with git repos

files = glob.glob(args.globpath,  recursive=True)
first_file = files.pop()
git_repo_df = sqlcontext.read.json(first_file)
git_repo_df = parsers.df_parse_email(git_repo_df,  "author_email")
git_repo_df.first()
for tmp_df_path in files:
    tmp_git_repo_df = sqlcontext.read.json(tmp_df_path)
    tmp_git_repo_df = parsers.df_parse_email(tmp_git_repo_df,  "author_email")
    final_df = git_repo_df.unionByName(tmp_git_repo_df)

final_df.write.json(args.outpath)
