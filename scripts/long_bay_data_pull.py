import os
import sys
import requests
from datetime import datetime, timedelta
from dateutil.relativedelta import *
import optparse


'''
http://hydrometcloud.com/hydrometcloud/customreportcontroller?action=Excel&siteId=796&sensorId=170966,170972,170973,170969,170970,170974,170968,170967,170971,173457,170964,170955,170956,170965,170954,170953,170957,170958,170959,170960,170961,170962,170963&dataOrder=descending&startDate=2019-01-01 00:00&endDate=2019-04-01 23:59&displayType=StationSensor&predefFlag=false&enddateFlag=false&now=Fri Apr 01 2022 15:10:25 GMT-0400 (Eastern Daylight Time)&predefval=lasttwodays
http://hydrometcloud.com/hydrometcloud/customreportcontroller?action=Excel&siteId=796&sensorId=170966,170972,170973,170969,170970,170974,170968,170967,170971,173457,170964,170955,170956,170965,170954,170953,170957,170958,170959,170960,170961,170962,170963&dataOrder=descending&startDate=2019-01-01 00:00&endDate=2019-05-01 00=00&displayType=StationSensor&predefFlag=false&enddateFlag=false&now=Fri Apr 01 2022 15:53:41 GMT-0500 (Eastern Daylight Time)&predefval=lasttwodays'''

def main():
    parser = optparse.OptionParser()
    parser.add_option("--PierName", dest="pier_name",
                      help="Name of pier to query")
    parser.add_option("--OutputDirectory", dest="output_dir",
                      help="Directory to save results to.")
    parser.add_option("--StartDate", dest="start_date",
                      help="")
    parser.add_option("--EndDate", dest="end_date",
                      help="")
    (options, args) = parser.parse_args()
    '''
    http://hydrometcloud.com/hydrometcloud/customreportcontroller?
    action=Excel&
    siteId=796&
    sensorId=170966,170972,170973,170969,170970,170974,170968,170967,170971,173457,170964,170955,170956,170965,170954,170953,170957,170958,170959,170960,170961,170962,170963&
    dataOrder=descending&
    startDate=2019-01-01%2000:00&
    endDate=2019-04-01%2023:59&
    displayType=StationSensor&
    predefFlag=false&
    enddateFlag=false&
    now=Fri%20Apr%2001%202022%2015:10:25%20GMT-0400%20(Eastern%20Daylight%20Time)&
    predefval=lasttwodays    
    '''
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
    '''
    http://hydrometcloud.com/hydrometcloud/customreportcontroller?
    action=Excel&
    siteId=797&
    sensorId=170975,170987,170988,170984,170985,170989,170983,170982,170986,170995,170996,170992,170993,170997,170991,170990,170994,170976,170977,170978,170979,170980,170981&
    dataOrder=descending&
    startDate=2022-04-04%2000:00:00&
    endDate=2022-04-04%2009:05:40&
    displayType=StationSensor&
    predefFlag=true&
    enddateFlag=false&
    now=Mon%20Apr%2004%202022%2009:05:40%20GMT-0400%20(Eastern%20Daylight%20Time)&
    predefval=lasttwodays
    '''
    '''
    
    '''

    sensor_ids = {
        'cherry_grove': {
            'sensor_ids': [170975,170987,170988,170984,170985,170989,170983,170982,170986,170995,170996,170992,170993,170997,170991,170990,170994,170976,170977,170978,170979,170980,170981],
            'site_id': 797
        },
        'apache': {
            'sensor_ids': [170966, 170972, 170973, 170969, 170970, 170974, 170968, 170967, 170971, 173457, 170964,
                           170955, 170956, 170965, 170954, 170953, 170957, 170958, 170959, 170960, 170961, 170962,
                           170963],
            'site_id': 796
        }

    }
    begin_date = datetime.strptime(options.start_date, '%Y-%m-%d %H:%M:%S')
    end_date = datetime.strptime(options.end_date, '%Y-%m-%d %H:%M:%S')

    current_start_date = begin_date
    for station in sensor_ids.keys():
        if station == options.pier_name:
            #Grab 2 months at a time
            get_data = True
            while get_data:
                if current_start_date < end_date:
                    if len(sensor_ids[station]):
                                                                                                   #Fri Feb 14 2020 09:14:38 GMT-0500 (Eastern Standard Time)&
                        now_time = "%s GMT-0500 (Eastern Daylight Time)" % (datetime.now().strftime('%a %b %d %Y %H:%M:%S'))
                        stop_date = current_start_date + relativedelta(months=2)
                        url = "action={action}&siteId={siteId}&sensorId={sensorId}&dataOrder={dataOrder}&startDate={startDate}&endDate={endDate}&displayType={displayType}&predefFlag={predefFlag}&enddateFlag={enddateFlag}&now={now}".format(action= 'Excel',
                            siteId= sensor_ids[station]['site_id'],
                            sensorId= ','.join(str(x) for x in sensor_ids[station]['sensor_ids']),
                            startDate= current_start_date.strftime("%Y-%m-%d %H:%M"),
                            dataOrder='ascending',
                            endDate= stop_date.strftime("%Y-%m-%d %H:%M"),
                            displayType= 'StationSensor',
                            predefFlag= 'false',
                            enddateFlag= 'false',
                            now= now_time,
                            predefval= 'lasttwodays')
                        '''
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
                        '''
                        full_url = "%s?%s" % (base_url, url)
                        print("Querying: %s\n" % (full_url))
                        req = requests.get(full_url)
                        if req.status_code == 200:
                            filename = "%s-%s-to-%s.xls" % (station,
                                                            current_start_date.strftime("%Y-%m-%d-%H_%M"),
                                                            stop_date.strftime("%Y-%m-%d-%H_%M"))
                            output_filename = os.path.join(options.output_dir, filename)
                            print("Saving file: %s\n" % (output_filename))
                            with open(output_filename, 'wb') as f:
                                for chunk in req.iter_content(chunk_size=1024):
                                    if chunk:  # filter out keep-alive new chunks
                                        f.write(chunk)
                    current_start_date = stop_date

    return

if __name__ == "__main__":

    main()