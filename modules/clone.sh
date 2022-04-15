GIT_REPOS=`jq -r ".[]" ../data/git_repos.json`
mkdir ../data
mkdir ../data/cloned-repos
cd ../data/cloned-repos
echo $GIT_REPOS
for REPO in $GIT_REPOS
do
    echo $REPO
    git clone $REPO
done