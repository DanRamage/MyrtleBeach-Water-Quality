#!/bin/bash

source /usr/local/virtualenv/pyenv-2.7.11/bin/activate;

cd /home/xeniaprod/scripts/MyrtleBeach-Water-Quality/scripts;

python build_historical_database.py --ConfigFile=/home/xeniaprod/scripts/MyrtleBeach-Water-Quality/config/mb_historic_data_config.ini --DataFile="/home/xeniaprod/scripts/MyrtleBeach-Water-Quality/data/historic/CherryGrove Pier 05112012 to 06052014.csv" --Header="Date,Time,Bottom Depth,Bottom Temp,Bottom Salinity ,Bottom DO Conc,Bottom DO Saturation ,Bottom pH,Bottom  Chlorophyll,Bottom Turbidity,Surface Depth,Surface Temp ,Surface Salinity ,Surface DO Conc,Surface DO saturation ,Surface pH,Surface Chlorophyll,Surface Turbidity+,Air Temp ,Barometric Pressure,Relative Humidity,Wind Direction ,Wind Speed,Rainfall" --FirstDataRow=2 --PlatformName=lbhmc.CherryGrove.pier

python build_historical_database.py --ConfigFile=/home/xeniaprod/scripts/MyrtleBeach-Water-Quality/config/mb_historic_data_config.ini --DataFile="/home/xeniaprod/scripts/MyrtleBeach-Water-Quality/data/historic/CherryGrove Pier 060414 to 123114.csv" --Header="Date,Time,Depth,Temp,Salinity,ODO Conc,ODO%,pH,Chlorophyll,Turbidity+,Depth,Temp,Salinity,ODO Conc,ODO%,pH,Chlorophyll,Turbidity+,Air Temp,BP,RH,Wind Direction Ave,Rainfall,Wind Speed" --FirstDataRow=11 --PlatformName=lbhmc.CherryGrove.pier

python build_historical_database.py --ConfigFile=/home/xeniaprod/scripts/MyrtleBeach-Water-Quality/config/mb_historic_data_config.ini --DataFile="/home/xeniaprod/scripts/MyrtleBeach-Water-Quality/data/historic/CherryGrove Pier 010115 to 071015.csv" --Header="Date,Time,Depth,Temp,Salinity,ODO Conc,ODO%,pH,Chlorophyll,Turbidity+,Depth,Temp,Salinity,ODO Conc,ODO%,pH,Chlorophyll,Turbidity+,Air Temp,BP,RH,Wind Direction Ave,Rainfall,Wind Speed" --FirstDataRow=11 --PlatformName=lbhmc.CherryGrove.pier

