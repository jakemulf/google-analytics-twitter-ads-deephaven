#!/bin/sh

##Example usage: DAYS_OFFSET=0 SCHEDULED=true . ./cron.sh
sh start.sh -d
python -m venv venv
source venv/bin/activate
pip install -r scripts/requirements.txt
python scripts/run_scripts.py scripts/ga_main.py,scripts/twitter_main.py,scripts/slack_main.py,scripts/parquet_writer.py,scripts/scheduler.py localhost 10000 30
docker-compose down
deactivate
rm -rf venv
