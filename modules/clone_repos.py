from gitindexer import index_git_repos, get_repo_metadata_remote, get_repo_metadata_path

index_git_repos("./git_repos.json", "commit", "../data/cloned-repos2/")