# udacity-capstone

## Getting the datasets

**Reddit data**

* Go to [Reddit Daily Exports](https://files.pushshift.io/reddit/comments/daily/) and download one of the exports, in my case RC_2018-01-01
* Extract file
* Use aws CLI to copy file to S3


**Git Data**

* Clone a tone of repos into, `./data/cloned-repos`
* Run `index_test_repos.py` 
* Recursively copy `./out/git_out/` to S3


## Copying files to S3

``` bash
mybucketname=paul-udacity-capstone
aws s3 cp ./data/cloned-repos s3://$mybucketname/git-dump/RC_2018_01_01
aws s3 cp ./RC_2018-01-01 s3://$mybucketname/reddit/2018
```

## Reminders

* reddit/RC_2018_01_01 is 1.5 Gb
  * s3a://paul-udacity-capstone/RC_2018_01_01
  * 2,360,226 lines in file and therefore posts
