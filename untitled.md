# Reminds

``` bash
aws s3 ls s3://paul-udacity-capstone

aws s3 cp reddit_paths.json s3://paul-udacity-capstone/reddit_paths.json
aws s3 cp git_paths.json s3://paul-udacity-capstone/git_paths.json
aws s3 cp --recursive ./out/git_out/commits/ s3://paul-udacity-capstone/git-dump
# aws s3 rm --recursive s3://paul-udacity-capstone/git-dump
```

mkdir reddit_split
split -l 1000 RC_2018-01-01 reddit_split/reddit_2018_01_01_