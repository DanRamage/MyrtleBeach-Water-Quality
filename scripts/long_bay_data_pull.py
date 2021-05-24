import sys
import requests
from datetime import datetime, timedelta
from dateutil.relativedelta import *





def main():

    base_url = 'http://hydrometcloud.com/hydrometcloud/customreportcontroller'
    '''
    action=Excel&
    siteId=796&
    sensorId=170966,170972,170973,170969,170970,170974,170968,170967,170971,173457,170964,170955,170956,170965,170954,170953,170957,170958,170959,170960,170961,170962,170963&
    dataOrder=descending&
    startDate=2016-01-01 00:00&
    endDate=2016-02-01 23:59&
    displayType=StationSensor&
    predefFlag=false&
    enddateFlag=false&
    now=Fri Feb 14 2020 09:14:38 GMT-0500 (Eastern Standard Time)&
    predefval=lasttwodays
    '''
    sensor_ids = {
        'apache': {
            'sensor_ids': [170966,170972,170973,170969,170970,170974,170968,170967,170971,173457,170964,170955,170956,170965,170954,170953,170957,170958,170959,170960,170961,170962,170963],
            'site_id': 796
        }
    }
    begin_date = datetime.strptime('2016-01-01 00:00:00', '%Y-%m-%d %H:%M:%S')
    end_date = datetime.strptime('2020-01-01 00:00:00', '%Y-%m-%d %H:%M:%S')

    current_start_date = begin_date
    for station in sensor_ids.keys():
        #Grab 4 months at a time
        get_data = True
        while get_data:
            if current_start_date < end_date:
                if len(sensor_ids[station]):
                    now_time = "%s GMT-0500 (Eastern Standard Time)" % (datetime.now().strftime('%b %d %Y %H:%M:%S'))
                    stop_date = current_start_date + relativedelta(months=4)
                    params = {
                        'action': 'Excel',
                        'siteId': sensor_ids[station]['site_id'],
                        'sensorId': ','.join(str(x) for x in sensor_ids[station]['sensor_ids']),
                        'startDate': current_start_date.strftime("%Y-%m-%d %H:%M"),
                        'endDate': stop_date.strftime("%Y-%m-%d %H:%M"),
                        'displayType': 'StationSensor',
                        'predefFlag': 'false',
                        'enddateFlag': 'false',
                        'now': now_time,
                        'predefval': 'lasttwodays'

                    }
                    req = requests.get(base_url, params=params)
                    if req.status == 200:
                        req
                current_start_date = stop_date

    return

if __name__ == "__main__":
    main()