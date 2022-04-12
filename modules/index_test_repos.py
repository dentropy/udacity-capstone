from gitindexer import index_git_repos, get_repo_metadata_remote, get_repo_metadata_path

# index_git_repos("../files/repo_list.json", ["commits"], "../out/git_out/", "../out/git_errors/")

# get_repo_metadata_path("../data/repos/bips", "commits", "../out/git_out/")
# get_repo_metadata_path("../data/repos/EIPs", "commits", "../out/git_out/")
# get_repo_metadata_path("../data/repos/CIPs", "commits", "../out/git_out/")

import glob
git_paths = glob.glob("../data/cloned-repos/**",)
for git_path in git_paths:
    print("Indexing", git_path)
    get_repo_metadata_path(git_path, "commits", "../out/git_out/")
