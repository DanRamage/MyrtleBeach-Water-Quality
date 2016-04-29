#!/bin/bash

source /usr2/virtualenvs/pyenv2.7/bin/activate;

cd /home/xeniaprod/scripts/MyrtleBeach-Water-Quality/scripts;

python mb_wq_prediction_engine.py --ConfigFile=/Users/danramage/Documents/workspace/WaterQuality/MyrtleBeach-Water-Quality/config/mb_prediction_config.ini >> /home/xeniaprod/tmp/log/mb_wq_prediction_engine_sh.log 2>&1
