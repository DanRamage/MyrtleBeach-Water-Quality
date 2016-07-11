#!/bin/bash

source /usr/local/virtualenv/pyenv-2.7.11/bin/activate;

python /home/xeniaprod/scripts/commonfiles/python/wqXMRGProcessing.py --ConfigFile=/home/xeniaprod/scripts/MyrtleBeach-Water-Quality/config/mb_historic_data_config.ini --ImportData=/mnt/waterquality/xmrg/2009/serfc_012009,/mnt/waterquality/xmrg/2009/serfc_022009,/mnt/waterquality/xmrg/2009/serfc_032009,/mnt/waterquality/xmrg/2009/serfc_042009,/mnt/waterquality/xmrg/2009/serfc_052009,/mnt/waterquality/xmrg/2009/serfc_062009,/mnt/waterquality/xmrg/2009/serfc_072009,/mnt/waterquality/xmrg/2009/serfc_082009,/mnt/waterquality/xmrg/2009/serfc_092009,/mnt/waterquality/xmrg/2009/serfc_102009,/mnt/waterquality/xmrg/2009/serfc_112009,/mnt/waterquality/xmrg/2009/serfc_122009