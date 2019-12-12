:<<BATCH

::
:: SPIT-Browser Scheduler: Runner
::

@echo off

if %1. == prod. (
  set FLASK_APP=scheduler.py
  set FLASK_ENV=production
  flask run --host=0.0.0.0
) else (
  set FLASK_APP=scheduler.py
  set FLASK_ENV=development
  flask run
)
exit

BATCH

if [ "$1" == "prod" ]; then
  export FLASK_APP=clienthttp.py
  export FLASK_ENV=production
  flask run --host=0.0.0.0
else
  export FLASK_APP=scheduler.py
  export FLASK_ENV=development
  flask run
fi
