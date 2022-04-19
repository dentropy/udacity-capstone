import pyspark.sql.functions as F
import pyspark.sql.types as T


# Functions for UDFs
# Source https://stackoverflow.com/questions/839994/extracting-a-url-in-python
def extract_urls(s):
    import re
    results = re.findall("(?P<url>https?://[^\s]+)", s)
    if results == []:
        return "NONE"
    else:
        return results[0]
udf_extract_urls = F.udf(extract_urls, T.StringType())
    
def extract_domain_name():
    import re
    results = re.findall(r'^(?:http:\/\/|www\.|https:\/\/)([^\/]+)', s)
    if results == []:
        return "NONE"
    else:
        return results[0]
udf_extract_domain_name = F.udf(extract_domain_name, T.StringType())

def extract_user_email():
    import re
    results = re.findall(r'([^@]+)', s)
    if results == []:
        return "NONE"
    else:
        return results[0]
udf_extract_user_email = F.udf(extract_user_email, T.StringType())

def extract_domain_name_email():
    import re
    results = re.findall(r'@(.*)', s)
    if results == []:
        return "NONE"
    else:
        return results[0]
udf_extract_domain_name_email = F.udf(extract_domain_name_email, T.StringType())