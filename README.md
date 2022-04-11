# udacity-capstone

``` bash
rm -rf ./out/DATA*

spark-submit ./index-git.py \
    --globpath $(pwd)/out/git_out/**/*.json \
    --outpath $(pwd)/out/DATA_GIT \
    --master spark://127.0.0.1:7077

spark-submit ./index-reddit.py \
    --inpath  $(pwd)/data/reddit-stuff/xaa \
    --outpath $(pwd)/out/DATA_REDDIT

spark-submit ./integrate-git-and-reddit.py \
    --gitin $(pwd)/out/DATA_GIT \
    --redditin $(pwd)/out/DATA_REDDIT \
    --outpath $(pwd)/out/DATA_INTEGRATED
```

## Copying files to S3

``` bash

mybucketname=paul-udacity-capstone
aws s3 cp --recursive  ./out/git_out/commits/ s3://$mybucketname/git-dump

aws s3 cp ./RC_2018-01-01 s3://$mybucketname/reddit/RC_2018_01_01
aws s3 cp ./RC_2020-01-01 s3://$mybucketname/reddit/RC_2020_01_01
```

## Reminders

* reddit/RC_2018_01_01 is 1.5 Gb
  * s3a://paul-udacity-capstone/RC_2018_01_01
* reddit/RC_2020_01_01 is 5.2 Gb
