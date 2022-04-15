import shutil
import subprocess
import json
import pathlib
import sys
import shlex

def check_mergestat():
    # Check if merge stat installed otherwise 
    if shutil.which("mergestat") == "":
        # TODO install it to local path is not installed
        raise Exception("mergestat not installed in path check out https://www.mergestat.com/")
    return True

def parse_path(repo_url, saving_path, insert_string):
    # Parse the path to save data to
    repo_path_split = repo_url.split("/")[2:]
    file_name = repo_path_split[-1].split(".")[0] + ".json"
    output_path = saving_path + "/" + insert_string + "/" + repo_path_split[-2]
    output_file_path = output_path + "/" + file_name
    return output_path, output_file_path

def get_repo_metadata_remote(repo_url, table_name, saving_path="/tmp/"):
    # Use mergestat to get emails, names, and add in git repo URL as JSON

    check_mergestat()

    # Create path if not exists
    output_path, output_file_path = parse_path(repo_url, saving_path, table_name)
    p = pathlib.Path(output_path)
    p.mkdir(parents=True, exist_ok=True)

    # Export the commits to file system
    
    bashCommand = """
    mergestat "SELECT COUNT(*), author_name, author_email, '{repo_url}' as remote_url from {table_name}('{repo_url}') GROUP BY author_email" --format ndjson > {output_file_path}""".format(repo_url=repo_url, output_file_path=output_file_path, table_name=table_name)
    print(bashCommand)
    process = subprocess.run([bashCommand], shell=True, check=True, stdout=subprocess.PIPE).stdout
    return output_file_path

def get_repo_metadata_path(repo_path, table_name, saving_path="/tmp/"):
    # Use mergestat to get emails, names, and add in git repo URL as JSON

    check_mergestat()
    bashCommand = """cd {repo_path} && git remote -v""".format(repo_path=repo_path)
    process = subprocess.run([bashCommand], shell=True, check=True, stdout=subprocess.PIPE).stdout
    repo_url = str(process).split("\\t")[1].split()[0]
    print(repo_url)
    
    output_path, output_file_path = parse_path(repo_url, saving_path, table_name)
    p = pathlib.Path(output_path)
    p.mkdir(parents=True, exist_ok=True)

    # Export the commits to file system
    
    bashCommand = """
    mergestat "SELECT COUNT(*) as commits, author_name, author_email, '{repo_url}' as remote_url from {table_name}() GROUP BY author_email" --repo {repo_path} --format ndjson > {output_file_path}""".format(repo_path=repo_path, repo_url=repo_url, output_file_path=output_file_path, table_name=table_name)
    print(bashCommand)
    process = subprocess.run([bashCommand], shell=True, check=True, stdout=subprocess.PIPE).stdout
    return output_file_path


def index_git_repos(path_to_json, saving_path="/tmp/", error_path="/tmp/"):
    try:
        repos_list = json.load(open(path_to_json))
    except:
        raise Exception("Your JSON list of git repo URLs is not formatted correctly")
    if type(repos_list) != list:
        raise Exception("Your JSON list of git repo URLs is not a raw list")
    for repo_url in repos_list:
        try:
            # print(saving_path)
            get_repo_metadata(repo_url, "commits", saving_path)
        except Exception as e:
            print("Error on", repo_url)
            print(e)