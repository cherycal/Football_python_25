import sys
sys.path.append('../modules')
from git import Repo
import tools

# rorepo is a Repo instance pointing to the git-python repository.
# For all you know, the first argument to Repo is a path to the repository
# you want to work with
# To find .git directory run git rev-parse --show-toplevel
# /media/sf_Shared/first

platform = tools.get_platform()
if (platform == "Windows"):
    repo = Repo("C:\\Ubuntu\\Shared\\FFB")
elif (platform == "Linux" or platform == "linux"):
    repo = Repo("/media/sf_Shared/first")
else:
    print("OS platform " + platform + " isn't Windows or Linux. Exit.")
    exit(1)

assert not repo.bare

file = "site/score.txt"

git = repo.git

# git.pull()

git.add(file)

git.commit('-m','update',file)

git.push()

print(git.status())


