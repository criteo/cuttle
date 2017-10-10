#!/usr/bin/env bash
# This script creates a pre-commit git hook that run scalafmt before each commit
# http://scalameta.org/scalafmt/
# Need scalafmt installed in CLI mode.
# Tested in Ubuntu's bash.
[ -d .git/hooks/ ] || (echo "It's not a git directory";exit 1)
[ -d .git/hooks/ ] && echo '#!/usr/bin/env bash
echo -e "\e[0;33m Scalafmt RUNNING \e[0m"
scalafmt --git true --diff-branch $(git branch | grep \* | cut -d " " -f2)
RESULT=$?
if [ ${RESULT} -ne 0 ]; then
    echo -e "\e[0;31m Scalafmt FAILED \e[0m"
    exit ${RESULT}
fi
echo -e "\e[0;32m Scalafmt SUCCEEDED \e[0m"
echo -e "\e[0;33m Add formating to commit\e[0m"
git add $(git diff-index --cached HEAD --name-only --diff-filter=d)
echo -e "\e[0;32m Add formating SUCCEEDED\e[0m"
exit 0
' > .git/hooks/pre-commit
chmod +x .git/hooks/pre-commit
