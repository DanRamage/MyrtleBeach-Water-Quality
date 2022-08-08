#!/bin/bash

source /usr/local/virtualenv/pyenv-3.8.5/bin/activate

cd /home/xeniaprod/scripts/MyrtleBeach-Water-Quality/scripts;

python mb_wq_prediction_engine.py --ConfigFile=/home/xeniaprod/scripts/MyrtleBeach-Water-Quality/config/mb_prediction_config.ini >> /home/xeniaprod/tmp/log/mb_wq_prediction_engine_sh.log 2>&1
