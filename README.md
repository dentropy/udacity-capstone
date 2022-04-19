# udacity-capstone

## Desciption

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

## Who is going to use the data model, think of user persona that you think will benefit from this?

There are already startups out there harvesting email addresse from github repositories in order to sell people stuff.[Source](https://news.ycombinator.com/item?id=30977883) Connecting those email addresses with other social media data allows for better valuation of the value that can be extracted from each email address.

For me personally I want to better understand the relationships between individuals and the groups they attach themselves to and how they overlap. For example I find a corperate entity that is capable of using the same domain name for their website as well as their email has a certain level of competence that should be noted.


## What are that types of questions this data model can help answer?

* Can we find the same domain name being shared for both email addresses and normal websites
* Total commits per domain name, total commits per email address, total commits per git organization, most contributors per git organization, most contributors per repo 
* Can we find people using the same username across email domains as well as reddit accounts?

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

## Data Model

**reddit_comments**

| Field Name       | Datatype | Constraint  | Description                               |
|------------------|----------|-------------|-------------------------------------------|
| index            | int      | Primary Key | Unique identifier for joins and lookups   |
| author           | text     | None        | Reddit username of who posted comment     |
| body             | text     | None        | Contents of text                          |
| controversiality | int      | None        | Score used to rank comment                |
| created_utc      | int      | None        | Timestamp when posted                     |
| edited           | int      | None        | Timestamp when edited                     |
| id               | text     | None        | Unique ID to relate to other comments     |
| link_id          | text     | None        | Formatted ID to relate to other comments  |
| parent_id        | text     | None        | link_id that this comment was replying to |
| permalink        | text     | None        | path to comment to share                  |
| subreddit        | text     | None        | Group comment was posted in               |
| url              | text     | None        | URL extracted from body                   |
| domain_name      | text     | None        | Domain name extracted from URL            |



**git_metadata**

| Field Name     | Datatype | Constraint  | Description                             |
|----------------|----------|-------------|-----------------------------------------|
| index          | int      | Primary Key | Unique identifier for joins and lookups |
| author_email   | text     | None        | Email of person who commited to repo    |
| author_name    | text     | None        | Name of person who commmited to repo    |
| commits        | int      | None        | Number of commits in this repo          |
| remote_url     | int      | None        | Remote URL to clone this repo           |
| email_username | int      | None        | Parsed username from this repo          |
| email_domain   | text     | None        | Parsed domain name from this repo       |



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