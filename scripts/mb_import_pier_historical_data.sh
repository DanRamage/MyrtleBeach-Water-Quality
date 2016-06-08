#!/bin/bash

source /usr/local/virtualenv/pyenv-2.7.11/bin/activate;

cd /home/xeniaprod/scripts/MyrtleBeach-Water-Quality/scripts;

python build_historical_database.py --ConfigFile=/home/xeniaprod/scripts/MyrtleBeach-Water-Quality/config/mb_historic_data_config.ini --DataFile="/home/xeniaprod/scripts/MyrtleBeach-Water-Quality/data/historic/2nd Ave Pier 010115 to 071015.csv" --Header="Date,Time,Depth,Temp,Salinity,ODO Conc,ODO%,pH,Chlorophyll,Turbidity+,Depth,Temp,Salinity,ODO Conc,ODO%,pH,Chlorophyll,Turbidity+,Air Temp,BP,RH,Wind Dir,Wind Speed,Rainfall" --FirstDataRow=11 --PlatformName=lbhmc.2ndAveNorth.pier
