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

spark-submit ./etl.py \
    --packages org.apache.hadoop:hadoop-aws:2.7.7 \
    --conf "spark.jars.packages=org.apache.hadoop:hadoop-aws:2.7.3" \
    --master spark://pop-os.localdomain:7077
```