#!/bin/bash

source /usr/local/virtualenv/pyenv-2.7.11/bin/activate;

python /home/xeniaprod/scripts/Florida-Water-Quality/commonfiles/python/wqXMRGProcessing.py --ConfigFile=/home/xeniaprod/scripts/MyrtleBeach-Water-Quality/config/mb_config.ini --FillGaps --BackfillNHours=192