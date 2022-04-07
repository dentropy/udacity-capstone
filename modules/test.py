from gitindexer import index_git_repos, get_repo_metadata_remote, get_repo_metadata_path

# index_git_repos("../files/repo_list.json", ["commits"], "../out/git_out/", "../out/git_errors/")

get_repo_metadata_path("/home/paul/Projects/DataEngineering/Capstone/out/repos/bips", "commits", "../out/git_out/")
get_repo_metadata_path("/home/paul/Projects/DataEngineering/Capstone/out/repos/EIPs", "commits", "../out/git_out/")

