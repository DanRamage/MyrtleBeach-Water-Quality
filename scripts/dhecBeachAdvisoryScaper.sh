#!/bin/bash

processType=$1


startTime=`date -u`
echo "Start time: $startTime\n" > /home/xeniaprod/tmp/log/dhecBeachAdvisoryScraperShellScript.log 2>&1

source /usr/local/virtualenv/pyenv-2.7.11/bin/activate

python --version >> /home/xeniaprod/tmp/log/dhecBeachAdvisoryScraperShellScript.log 2>&1
if [ "$processType" = "webscraperesults" ]
  then
  echo $processType
  python /home/xeniaprod/scripts/dhec/dhecBeachAdvisoryReader.py --ConfigFile=/home/xeniaprod/config/dhecBeachAdvisoryApp.ini > /home/xeniaprod/tmp/log/dhecBeachAdvisoryScraperShellScript.log 2>&1

elif  [ "$processType" = "createstationdata" ]
  then
  echo $processType
  python /home/xeniaprod/scripts/dhec/dhecBeachAdvisoryReader.py --ImportStationsFile=/home/xeniaprod/scripts/dhec/SamplingStations.csv --ConfigFile=/home/xeniaprod/config/dhecBeachAdvisoryApp.ini > /home/xeniaprod/tmp/log/dhecBeachAdvisoryScraperShellScript.log 2>&1

fi
startTime=`date -u`
echo "\nEnd time: $startTime" >> /home/xeniaprod/tmp/log/dhecBeachAdvisoryScraperShellScript.log 2>&1
