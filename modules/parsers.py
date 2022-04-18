import psycopg2
import pyspark.sql.functions as F
import glob
import pandas as pd
# Extract a URL from a string

## df_extract_url
#  * Take input of a dataframe
#  * Takes a column name to extract url from body of text
# Regex Source: https://stackoverflow.com/questions/28185064/python-infinite-loop-in-regex-to-match-url
def df_extract_url(tmp_df, tmp_col):
    tmp_df = tmp_df.withColumn("url", F.regexp_extract(F.col(tmp_col), r"(?i)\b((?:https?://|www\d{0,3}[.]|[a-z0-9.\-]+[.][a-z]{2,4}/)(?:[^\s()<>]+|\(([^\s()<>]+|(\([^\s()<>]+\)))*\))+(?:\(([^\s()<>]+|(\([^\s()<>]+\)))*\)|[^\s`!()\[\]{};:'\".,<>?«»“”‘’]))", 0))          
    return tmp_df

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



def contact_sql_tables(sql_con_string, first_table_name, second_table_name):
    tmp_conn = psycopg2.connect(sql_con_string)
    tmp_cur = tmp_conn.cursor()
    column_names = list(pd.read_sql_query("select * from {} WHERE False;".format(first_table_name + "2"),sql_con_string))
    column_names_string = ",".join(column_names)
    concat_tables = ("""
    INSERT INTO {} ({})
    SELECT {}
    FROM {};
    """.format(first_table_name, column_names_string, column_names_string, second_table_name))
    tmp_cur.execute(concat_tables)
    tmp_conn.commit()
    

def json_glob_to_database(glob_path, sql_conn_string, table_name, sub_df=[]):
    files_list = glob.glob(glob_path)
    if len(files_list) == 1:   
        df = pd.read_json(files_list.pop(), lines=True)
        if sub_df != []:
            df = df[sub_df]
        df.to_sql(name=table_name, con=sql_conn_string)
    else:
        df = pd.read_json(files_list.pop(), lines=True)
        if sub_df != []:
            df = df[sub_df]
        df.to_sql(name=table_name, con=sql_conn_string)
        for json_file in files_list:
            df2 = pd.read_json(json_file, lines=True)
            if sub_df != []:
                df2 = df2[sub_df]
            df2.to_sql(name=(table_name + "2"), con=sql_conn_string)
            contact_sql_tables(sql_conn_string, table_name, (table_name + "2"))
            drop_tables(sql_conn_string, [(table_name + "2")])
        
        
def drop_tables(psql_conn_string, table_name_list):
    tmp_conn = psycopg2.connect(psql_conn_string)
    tmp_cur = tmp_conn.cursor()
    for table_name in table_name_list:
        tmp_sql_query =  """DROP TABLE IF EXISTS  {}""".format(table_name)
        tmp_cur.execute( tmp_sql_query )
        tmp_conn.commit()
        
        
def sql_extract_url(psql_conn_string, table_name, read_text_column_nane, write_text_column_name):
    tmp_conn = psycopg2.connect(psql_conn_string)
    tmp_cur = tmp_conn.cursor()
    alter_table_query = """
    ALTER TABLE {}
    ADD {} TEXT;
    """.format(table_name, write_text_column_name)
    try:
        tmp_cur.execute(alter_table_query)
        tmp_conn.commit()
    except:
        print("Table already created")
    insert_line = "INSERT INTO " + table_name + "(" + write_text_column_name + ")" + "\n"
    first_part_regex = "SELECT regexp_matches(" + read_text_column_nane + " , "
    # Source: https://stackoverflow.com/questions/27653554/regular-expression-for-website-url-with-different-types-using-javascript
    regex = r"""'(?:(?:https?|ftp):\/\/)(?:\S+(?::\S*)?@)?(?:(?!(?:10|127)(?:\.\d{1,3}){3})(?!(?:169\.254|192\.168)(?:\.\d{1,3}){2})(?!172\.(?:1[6-9]|2\d|3[0-1])(?:\.\d{1,3}){2})(?:[1-9]\d?|1\d\d|2[01]\d|22[0-3])(?:\.(?:1?\d{1,2}|2[0-4]\d|25[0-5])){2}(?:\.(?:[1-9]\d?|1\d\d|2[0-4]\d|25[0-4]))|(?:(?:[a-z\u00a1-\uffff0-9]-*)*[a-z\u00a1-\uffff0-9]+)(?:\.(?:[a-z\u00a1-\uffff0-9]-*)*[a-z\u00a1-\uffff0-9]+)*(?:\.(?:[a-z\u00a1-\uffff]{2,}))\.?)(?::\d{2,5})?(?:[/?#]\S*)?'"""
    from_line = "\nFROM " + table_name

    first_line = "UPDATE {} \n".format(table_name)
    second_line = "SET {} = (".format(write_text_column_name)
    third_line = "(SELECT regexp_matches({}.{}, ".format(table_name, read_text_column_nane)
    fourth_line = ")));"
    final_query = first_line +  second_line + third_line + regex + fourth_line
    insert_url_query = """
    UPDATE {} 
    SET {} = (
        (SELECT regexp_matches({}.{}, 
        '@(.*)'  
    )));
    """.format(table_name, write_text_column_name, table_name, read_text_column_nane)
    tmp_cur.execute(final_query)
    tmp_conn.commit()
    
def sql_extract_domain_from_url(psql_conn_string, table_name, read_text_column_nane, write_text_column_name):
    tmp_conn = psycopg2.connect(psql_conn_string)
    tmp_cur = tmp_conn.cursor()
    alter_table_query = """
    ALTER TABLE {}
    ADD {} TEXT;
    """.format(table_name, write_text_column_name)
    try:
        tmp_cur.execute(alter_table_query)
        tmp_conn.commit()
    except:
        print("Table already created")
    first_line = "UPDATE {} \n".format(table_name)
    second_line = "SET {} = (".format(write_text_column_name)
    third_line = "(SELECT regexp_matches({}.{}, ".format(table_name, read_text_column_nane)
    substr = "( select substring(url from '.*://([^/]*)') )"
    fourth_line = ")));"
    final_query = first_line +  second_line + substr + fourth_line
    insert_url_query = """
    UPDATE {} 
    SET {} = (
        (SELECT regexp_matches({}.{}, 
        '@(.*)'  
    )));
    """.format(table_name, write_text_column_name, table_name, read_text_column_nane)
    tmp_cur.execute(final_query)
    tmp_conn.commit()

def sql_extract_domain_from_url_regex(psql_conn_string, table_name, read_text_column_nane, write_text_column_name):
    tmp_conn = psycopg2.connect(psql_conn_string)
    tmp_cur = tmp_conn.cursor()
    alter_table_query = """
    ALTER TABLE {}
    ADD {} TEXT;
    """.format(table_name, write_text_column_name)
    tmp_cur.execute(alter_table_query)
    tmp_conn.commit()
    insert_line = "INSERT INTO " + table_name + "(" + write_text_column_name + ")" + "\n"
    first_part_regex = "SELECT regexp_matches(" + read_text_column_nane + " , "
    regex = r"""'.*\://?([^\/]+)'"""
    from_line = "\nFROM " + table_name
    first_line = "UPDATE {} \n".format(table_name)
    second_line = "SET {} = (".format(write_text_column_name)
    third_line = "(SELECT regexp_matches({}.{}, ".format(table_name, read_text_column_nane)
    fourth_line = ")));"
    final_query = first_line +  second_line + third_line + regex + fourth_line
    insert_url_query = """
    UPDATE {} 
    SET {} = (
        (SELECT regexp_matches({}.{}, 
        '@(.*)'  
    )));
    """.format(table_name, write_text_column_name, table_name, read_text_column_nane)
    tmp_cur.execute(final_query)
    tmp_conn.commit()

# Get the domain name of an email from an email address
def sql_parse_email_domainname(psql_conn_string, table_name, read_text_column_nane, write_text_column_name):
    tmp_conn = psycopg2.connect(psql_conn_string)
    tmp_cur = tmp_conn.cursor()
    alter_table_query = """
    ALTER TABLE {}
    ADD {} TEXT;
    """.format(table_name, write_text_column_name)
    tmp_cur.execute(alter_table_query)
    tmp_conn.commit()
    insert_url_query = """
    UPDATE {} 
    SET {} = (
        (SELECT regexp_matches({}.{}, '@(.*)'  ))
    );
    """.format(table_name, write_text_column_name, table_name, read_text_column_nane)
    tmp_cur.execute(insert_url_query)
    tmp_conn.commit()
    
    
# Get the domain name of an email from an email address
def sql_parse_email_username(psql_conn_string, table_name, read_text_column_nane, write_text_column_name):
    tmp_conn = psycopg2.connect(psql_conn_string)
    tmp_cur = tmp_conn.cursor()
    alter_table_query = """
    ALTER TABLE {}
    ADD {} TEXT;
    """.format(table_name, write_text_column_name)
    tmp_cur.execute(alter_table_query)
    tmp_conn.commit()
    insert_url_query = """
    UPDATE {} 
    SET {} = (
        (SELECT regexp_matches({}.{},  '([^@]+)'  ))
    );
    """.format(table_name, write_text_column_name, table_name, read_text_column_nane)
    tmp_cur.execute(insert_url_query)
    tmp_conn.commit()
