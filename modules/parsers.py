import pyspark.sql.functions as F
# Extract a URL from a string

## df_extract_url
#  * Take input of a dataframe
#  * Takes a column name to extract url from body of text
# Regex Source: https://stackoverflow.com/questions/28185064/python-infinite-loop-in-regex-to-match-url
def df_extract_url(tmp_df, tmp_col):
    return tmp_df.withColumn("url", F.regexp_extract(F.col(tmp_col), r"(?i)\b((?:https?://|www\d{0,3}[.]|[a-z0-9.\-]+[.][a-z]{2,4}/)(?:[^\s()<>]+|\(([^\s()<>]+|(\([^\s()<>]+\)))*\))+(?:\(([^\s()<>]+|(\([^\s()<>]+\)))*\)|[^\s`!()\[\]{};:'\".,<>?«»“”‘’]))", 0))          


# Get domain name from URL and put it in domainname table
# Regex Source: https://stackoverflow.com/questions/25703360/regular-expression-extract-subdomain-domain
def df_parse_domainname(tmp_df, tmp_col):
    tmp_df = tmp_df.withColumn( "domain_name", F.regexp_extract(F.col(tmp_col) , r'^(?:http:\/\/|www\.|https:\/\/)([^\/]+)', 1)) 
    return tmp_df

# Get the domain name of an email from an email address
def df_parse_email(tmp_df, tmp_col):
    tmp_df = tmp_df.withColumn("email_username" , F.regexp_extract(F.col(tmp_col),   r'([^@]+)', 1)) 
    tmp_df = tmp_df.withColumn("email_domain"   , F.regexp_extract(F.col(tmp_col) ,  r'@(.*)'  , 1)) 
    return tmp_df