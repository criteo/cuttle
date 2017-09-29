#!/usr/bin/env bash
# This script creates a pre-commit git hook that run scalafmt before each commit
# http://scalameta.org/scalafmt/
# Need scalafmt installed in CLI mode.
# Tested in Ubuntu's bash.
[ -d .git/hooks/ ] || (echo "It's not a git directory";exit 1)
[ -d .git/hooks/ ] && echo '#!/usr/bin/env bash
echo -e "\e[0;33m Scalafmt RUNNING \e[0m"
scalafmt --git true --diff-branch $(git branch | head -n1 | cut -d " "  -f2)
RESULT=$?
if [ ${RESULT} -ne 0 ]; then
    echo -e "\e[0;31m Scalafmt FAILED \e[0m"
    exit ${RESULT}
fi
echo -e "\e[0;32m Scalafmt SUCCEEDED \e[0m"
exit 0
' > .git/hooks/pre-commit
sudo chmod +x .git/hooks/pre-commit