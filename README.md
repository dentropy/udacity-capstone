# udacity-capstone

``` bash
rm -rf ./out/DATA*

spark-submit ./index-git.py \
    --globpath $(pwd)/out/git_out/**/*.json \
    --outpath $(pwd)/out/DATA_GIT

spark-submit ./index-reddit.py \
    --inpath  $(pwd)/data/reddit-stuff/xaa \
    --outpath $(pwd)/out/DATA_REDDIT

spark-submit ./integrate-git-and-reddit.py \
    --gitin $(pwd)/out/DATA_GIT \
    --redditin $(pwd)/out/DATA_REDDIT \
    --outpath $(pwd)/out/DATA_INTEGRATED
```