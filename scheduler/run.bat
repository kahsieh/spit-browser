::
:: Runner for SPIT-Browser Scheduler
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
