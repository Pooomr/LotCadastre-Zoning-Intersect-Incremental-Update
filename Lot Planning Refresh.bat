@ECHO OFF

ECHO --------------------------------------------

ECHO Testing SDE Connection to Planning DB

ECHO --------------------------------------------

ECHO User is %USERNAME%

cd /d "**Directory where scripts will be stored**\files"

ECHO Loading Python Environment files...

"**Python Exe location**\python.exe" "Lot Planning Refresh.py" %USERNAME%


PAUSE
