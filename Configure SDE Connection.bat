@ECHO OFF

ECHO ----------------------------------------------------------

ECHO Creating/Replace PlanningDB.SDE connection file...

ECHO ----------------------------------------------------------

cd /d "**Directory where scripts will be stored**\files"

"**Python Exe location**\python.exe" "Configure SDE Connection.py" %USERNAME%

PAUSE
