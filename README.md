## TODO

* Data Quality Checks
* Export JSON of all git remotes to github jist
* Get SQL database in the cloud, dump everything there
* Final project writeup
* Submit

# udacity-capstone

The purpose of this project is to develop a ETL pipeline for social media information, in this case reddit comments and git metadata. The unique quality across these datasets are domain names. Git metadata includes email addresses which use a domain name and website URL's can be extracted from reddit comments.

Two datasets were used for this project,

* Reddit comment export from [pushift.io](https://files.pushshift.io/reddit/comments/daily/), specically the RC_2018-01-01 export
* A custom list of git repos with metadata extracted using a custom script

* Cross dataset comparisons
  * Join domain name of git email with domain name of URL in reddit comment
  * Join username from git email with reddit username
* Inner dataset queries
  * Reddit
    * Most comments per user
    * Most comments per subreddit
  * Git
    * Groupby Email Address
    * Group by email address AND repo

## Requirements

* git
* python 3.6
  * packages
    * pyspark
    * pandas
    * jupyterlab
* Spark
* aws cli
  * S3 Bucket with READ/WRITE permissions
* [mergestat](https://github.com/mergestat/mergestat)

## Getting the datasets

**Reddit data**

* Go to [Reddit Daily Exports](https://files.pushshift.io/reddit/comments/daily/) and download one of the exports, in my case RC_2018-01-01
* Extract file
* Use aws CLI to copy file to S3


**Git Data**

* Clone a tone of repos into, `./data/cloned-repos`
* Run `index_test_repos.py` 
* Recursively copy `./out/git_out/` to S3
* To use same dataset
  ``` bash
  cd modules
  wget https://gist.githubusercontent.com/dentropy/57ec7aad8bbb746bb1d8afad93893e61/raw/57511ada36131f7e5113e3914eca9fe5eb0e4545/git_repos.json
  bash clone.sh
  python3 index_test_repos.py
  cd ..
  mybucketname=paul-udacity-capstone # CHANGE THIS
  aws s3 cp ./data/cloned-repos s3://$mybucketname/git-dump/RC_2018_01_01
  ```


## Copying datasets to S3

``` bash
mybucketname=paul-udacity-capstone
aws s3 cp ./data/cloned-repos s3://$mybucketname/git-dump/RC_2018_01_01
aws s3 cp ./RC_2018-01-01 s3://$mybucketname/reddit/2018
```

## Reminders

* reddit/RC_2018_01_01 is 1.5 Gb
  * s3a://paul-udacity-capstone/RC_2018_01_01
  * 2,360,226 lines in file and therefore posts

``` bash
aws s3 ls s3://paul-udacity-capstone

aws s3 cp reddit_paths.json s3://paul-udacity-capstone/reddit_paths.json
aws s3 cp git_paths.json s3://paul-udacity-capstone/git_paths.json
aws s3 cp --recursive ./out/git_out/commits/ s3://paul-udacity-capstone/git-dump
# aws s3 rm --recursive s3://paul-udacity-capstone/git-dump
```

mkdir reddit_split
split -l 1000 RC_2018-01-01 reddit_split/reddit_2018_01_01_