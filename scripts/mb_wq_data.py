import sys
sys.path.append('../commonfiles/python')

import logging.config

from datetime import datetime, timedelta
from pytz import timezone
from shapely.geometry import Polygon
import logging.config

#import netCDF4 as nc
import numpy as np
from bisect import bisect_left,bisect_right
import csv

from wqHistoricalData import wq_data
from wqXMRGProcessing import wqDB
from wqHistoricalData import station_geometry,sampling_sites, wq_defines, geometry_list
from date_time_utils import get_utc_epoch
from NOAATideData import noaaTideData
from xeniaSQLAlchemy import xeniaAlchemy, multi_obs, func
from xeniaSQLiteAlchemy import xeniaAlchemy as sl_xeniaAlchemy, multi_obs as sl_multi_obs, func as sl_func
from sqlalchemy import or_
from stats import calcAvgSpeedAndDirV2,calcAvgSpeedAndDir
from romsTools import closestCellFromPtInPolygon
from xenia import qaqcTestFlags
meters_per_second_to_mph = 2.23694


class mb_wq_site(station_geometry):
  def __init__(self, **kwargs):
    station_geometry.__init__(self, kwargs['name'], kwargs['wkt'])
    self.epa_id = kwargs['epa_id']
    self.description = kwargs['description']
    self.county = kwargs['county']
    return
"""
florida_sample_sites
Overrides the default sampling_sites object so we can load the sites from the florida data.
"""
class mb_sample_sites(sampling_sites):
  def __init__(self):
    self.logger = logging.getLogger(type(self).__name__)

  """
  Function: load_sites
  Purpose: Given the file_name in the kwargs, this will read the file and load up the sampling
    sites we are working with.
  Parameters:
    **kwargs - Must have file_name which is full path to the sampling sites csv file.
  Return:
    True if successfully loaded, otherwise False.
  """
  def load_sites(self, **kwargs):
    if 'file_name' in kwargs:
      if 'boundary_file' in kwargs:
        fl_boundaries = geometry_list(use_logger=True)
        fl_boundaries.load(kwargs['boundary_file'])

      try:
        header_row = ["WKT","EPAbeachID","SPLocation","Description","County","Boundary"]
        if self.logger:
          self.logger.debug("Reading sample sites file: %s" % (kwargs['file_name']))

        sites_file = open(kwargs['file_name'], "rU")
        dict_file = csv.DictReader(sites_file, delimiter=',', quotechar='"', fieldnames=header_row)
      except IOError as e:
        if self.logger:
          self.logger.exception(e)
      else:
        line_num = 0
        for row in dict_file:
          if line_num > 0:
            add_site = False
            #The site could be in multiple boundaries, so let's search to see if it is.
            station = self.get_site(row['SPLocation'])
            if station is None:
              add_site = True
              """
              station_geometry.__init__(self, kwargs['name'], kwargs['wkt'])
              self.epa_id = kwargs['epa_id']
              self.description = kwargs['description']
              self.county = kwargs['county']

              """
              station = mb_wq_site(name=row['SPLocation'],
                                        wkt=row['WKT'],
                                        epa_id=row['EPAbeachID'],
                                        description=row['Description'],
                                        county=row['County'])
              if self.logger:
                self.logger.debug("Processing sample site: %s" % (row['SPLocation']))
              self.append(station)
              try:
                boundaries = row['Boundary'].split(',')
                for boundary in boundaries:
                  if self.logger:
                    self.logger.debug("Sample site: %s Boundary: %s" % (row['SPLocation'], boundary))
                  boundary_geometry = fl_boundaries.get_geometry_item(boundary)
                  if add_site:
                    #Add the containing boundary
                    station.contained_by.append(boundary_geometry)
              except AttributeError as e:
                self.logger.exception(e)
          line_num += 1
        return True
    return False

"""
florida_wq_data
Class is responsible for retrieving the data used for the sample sites models.
"""
class mb_wq_historical_data(wq_data):
  """
  Function: __init__
  Purpose: Initializes the class.
  Parameters:
    boundaries - The boundaries for the NEXRAD data the site falls within, this is required.
    xenia_database_name - The full file path to the xenia database that houses the NEXRAD and other
      data we use in the models. This is required.
  """
  def __init__(self, **kwargs):
    wq_data.__init__(self, **kwargs)
    self.logger = logging.getLogger(__name__)

    self.site = None
    #The main station we retrieve the values from.
    self.tide_station = 8661070
    #These are the settings to correct the tide for the subordinate station.
    self.tide_data_file = kwargs['tide_data_file']
    self.tide_data_obj = None

    self.logger.debug("Connection to nexrad xenia db: %s" % (kwargs['xenia_nexrad_database_name']))
    self.nexrad_db = wqDB(kwargs['xenia_nexrad_database_name'], __name__)

    if self.logger:
      self.logger.debug("Connection to xenia db: %s" % (kwargs['xenia_database_name']))
    #self.xenia_db = wqDB(kwargs['xenia_database_name'], type(self).__name__)
    try:
      #Connect to the xenia database we use for observations aggregation.
      self.xenia_obs_db = sl_xeniaAlchemy()
      if self.xenia_obs_db.connectDB('sqlite', None, None, kwargs['xenia_database_name'], None, False):
        self.logger.info("Succesfully connect to DB: %s" %(kwargs['xenia_database_name']))
      else:
        self.logger.error("Unable to connect to DB: %s" %(kwargs['xenia_database_name']))

    except Exception as e:
      self.logger.exception(e)
      raise

    #List of platforms that we query same obs types from
    self.platforms = ['carocoops.SUN2.buoy', 'lbhmc.CherryGrove.pier', 'lbhmc.Apache.pier']
    #self.platforms = ['carocoops.SUN2.buoy', 'lbhmc.2ndAveNorth.pier', 'lbhmc.CherryGrove.pier', 'lbhmc.Apache.pier']

  def __del__(self):
    if self.logger:
      self.logger.debug("Closing connection to xenia db")
    self.xenia_obs_db.disconnect()

  def reset(self, **kwargs):
    self.site = kwargs['site']

  """
  Function: query_data
  Purpose: Retrieves all the data used in the modelling project.
  Parameters:
    start_data - Datetime object representing the starting date to query data for.
    end_date - Datetime object representing the ending date to query data for.
    wq_tests_data - A OrderedDict object where the retrieved data is store.
  Return:
    None
  """
  def query_data(self, start_date, end_date, wq_tests_data):
    if self.logger:
      self.logger.debug("Site: %s start query data for datetime: %s" % (self.site.name, start_date))

    self.initialize_return_data(wq_tests_data)

    self.get_tide_data(start_date, wq_tests_data)
    self.get_nexrad_data(start_date, wq_tests_data)
    #self.get_nos_data(start_date, wq_tests_data)
    for platform in self.platforms:
      var_name = platform.split('.')
      self.get_platform_data(start_date, platform, wq_tests_data, var_name[1].lower())

    if self.logger:
      self.logger.debug("Site: %s Finished query data for datetime: %s" % (self.site.name, start_date))

  """
  Function: initialize_return_data
  Purpose: INitialize our ordered dict with the data variables and assign a NO_DATA
    initial value.
  Parameters:
    wq_tests_data - An OrderedDict that is initialized.
  Return:
    None
  """
  def initialize_return_data(self, wq_tests_data):
    if self.logger:
      self.logger.debug("Creating and initializing data dict.")
    if self.tide_data_obj is None:
      self.tide_data_obj = {}
      #Build the historical tide data dict.
      with open(self.tide_data_file, 'r') as tide_data_file:
        header = ["Station","Date","Range","HH","HH Date","LL","LL Date", "Tide Stage"]
        data_csv = csv.DictReader(tide_data_file, fieldnames=header)
        line_num = 0
        for row in data_csv:
          if line_num:
            self.tide_data_obj[row['Date']] = {
              'station': row['Station'],
              'range': row['Range'],
              'hh': row['HH'],
              'hh_date': row['HH Date'],
              'll': row['LL'],
              'll_date': row['LL Date'],
              'tide_stage': row['Tide Stage']
            }
          line_num += 1
    #Build variables for the base tide station.
    var_name = 'tide_range_%s' % (self.tide_station)
    wq_tests_data[var_name] = wq_defines.NO_DATA
    var_name = 'tide_hi_%s' % (self.tide_station)
    wq_tests_data[var_name] = wq_defines.NO_DATA
    var_name = 'tide_lo_%s' % (self.tide_station)
    wq_tests_data[var_name] = wq_defines.NO_DATA
    var_name = 'tide_stage_%s' % (self.tide_station)
    wq_tests_data[var_name] = wq_defines.NO_DATA

    '''
    wq_tests_data['nos8661070_wind_spd'] = wq_defines.NO_DATA
    wq_tests_data['nos8661070_wind_dir_val'] = wq_defines.NO_DATA
    wq_tests_data['nos8661070_water_temp'] = wq_defines.NO_DATA
    wq_tests_data['nos8661070_water_level'] = wq_defines.NO_DATA
    '''
    for platform in self.platforms:
      platform_name = platform.split('.')
      var_name = '%s_water_temp' % (platform_name[1].lower())
      wq_tests_data[var_name] = wq_defines.NO_DATA
      var_name = '%s_salinity' % (platform_name[1].lower())
      wq_tests_data[var_name] = wq_defines.NO_DATA
      #if platform == 'carocoops.SUN2.buoy':
      var_name = '%s_wind_speed' % (platform_name[1].lower())
      wq_tests_data[var_name] = wq_defines.NO_DATA
      var_name = '%s_wind_dir_val' % (platform_name[1].lower())
      wq_tests_data[var_name] = wq_defines.NO_DATA
      var_name = "%s_wind_speed_u" % platform_name[1].lower()
      wq_tests_data[var_name] = wq_defines.NO_DATA
      var_name = "%s_wind_dir_val_v" % platform_name[1].lower()
      wq_tests_data[var_name] = wq_defines.NO_DATA

      if platform !='carocoops.SUN2.buoy':
        var_name = "%s_chlorophyl" % (platform_name[1].lower())
        wq_tests_data[var_name] = wq_defines.NO_DATA
        var_name = "%s_do_percent" % (platform_name[1].lower())
        wq_tests_data[var_name] = wq_defines.NO_DATA
        var_name = "%s_do_mg" % (platform_name[1].lower())
        wq_tests_data[var_name] = wq_defines.NO_DATA

    for boundary in self.site.contained_by:
      if len(boundary.name):
        for prev_hours in range(24, 192, 24):
          clean_var_boundary_name = boundary.name.lower().replace(' ', '_')
          var_name = '%s_nexrad_summary_%d' % (clean_var_boundary_name, prev_hours)
          wq_tests_data[var_name] = wq_defines.NO_DATA

        var_name = '%s_nexrad_dry_days_count' % (clean_var_boundary_name)
        wq_tests_data[var_name] = wq_defines.NO_DATA

        var_name = '%s_nexrad_rainfall_intensity' % (clean_var_boundary_name)
        wq_tests_data[var_name] = wq_defines.NO_DATA

        var_name = '%s_nexrad_total_1_day_delay' % (clean_var_boundary_name)
        wq_tests_data[var_name] = wq_defines.NO_DATA
        var_name = '%s_nexrad_total_2_day_delay' % (clean_var_boundary_name)
        wq_tests_data[var_name] = wq_defines.NO_DATA
        var_name = '%s_nexrad_total_3_day_delay' % (clean_var_boundary_name)
        wq_tests_data[var_name] = wq_defines.NO_DATA


    if self.logger:
      self.logger.debug("Finished creating and initializing data dict.")

    return


  def get_tide_data(self, start_date, wq_tests_data):
    if self.logger:
      self.logger.debug("Start retrieving tide data for station: %s date: %s" % (self.tide_station, start_date))

    if self.tide_data_obj is not None:
      date_key = start_date.strftime('%Y-%m-%dT%H:%M:%S')
      if date_key in self.tide_data_obj:
        tide_rec = self.tide_data_obj[date_key]
        wq_tests_data['tide_range_%s' % (self.tide_station)] = tide_rec['range']
        wq_tests_data['tide_hi_%s' % (self.tide_station)] = tide_rec['hh']
        wq_tests_data['tide_lo_%s' % (self.tide_station)] = tide_rec['ll']
        wq_tests_data['tide_stage_%s' % (self.tide_station)] = tide_rec['tide_stage']
        """
        try:
          #Get previous 24 hours.
          tide_start_time = (start_date - timedelta(hours=24))
          tide_end_time = start_date

          tide = noaaTideData(use_raw=True, logger=self.logger)

          tide_stage = tide.get_tide_stage(begin_date = tide_start_time,
                             end_date = tide_end_time,
                             station=self.tide_station,
                             datum='MLLW',
                             units='feet',
                             time_zone='GMT')
          wq_tests_data['tide_stage_%s' % (self.tide_station)] = tide_stage

        except Exception as e:
          if self.logger:
            self.logger.exception(e)
        """
      else:
        if start_date > datetime.strptime('2022-01-01', "%Y-%m-%d").astimezone(timezone('UTC')):
          start_date
        self.logger.error("Tide Data not found for: %s" % (date_key))
    if self.logger:
      self.logger.debug("Finished retrieving tide data for station: %s date: %s" % (self.tide_station, start_date))

    return

  def get_nexrad_data(self, start_date, wq_tests_data):
    if self.logger:
      self.logger.debug("Start retrieving nexrad data datetime: %s" % (start_date.strftime('%Y-%m-%d %H:%M:%S')))

    #Collect the radar data for the boundaries.
    for boundary in self.site.contained_by:
      clean_var_bndry_name = boundary.name.lower().replace(' ', '_')

      platform_handle = 'nws.%s.radarcoverage' % (boundary.name)
      if self.logger:
        self.logger.debug("Start retrieving nexrad platfrom: %s" % (platform_handle))
      # Get the radar data for previous 8 days in 24 hour intervals
      for prev_hours in range(24, 192, 24):
        var_name = '%s_nexrad_summary_%d' % (clean_var_bndry_name, prev_hours)
        radar_val = self.nexrad_db.getLastNHoursSummaryFromRadarPrecip(platform_handle,
                                                                    start_date,
                                                                    prev_hours,
                                                                    'precipitation_radar_weighted_average',
                                                                    'mm')
        if radar_val != None:
          #Convert mm to inches
          wq_tests_data[var_name] = radar_val * 0.0393701
        else:
          if self.logger:
            self.logger.error("No data available for boundary: %s Date: %s. Error: %s" %(var_name, start_date, self.nexrad_db.getErrorInfo()))

      #calculate the X day delay totals
      if wq_tests_data['%s_nexrad_summary_48' % (clean_var_bndry_name)] != wq_defines.NO_DATA and\
         wq_tests_data['%s_nexrad_summary_24' % (clean_var_bndry_name)] != wq_defines.NO_DATA:
        wq_tests_data['%s_nexrad_total_1_day_delay' % (clean_var_bndry_name)] = wq_tests_data['%s_nexrad_summary_48' % (clean_var_bndry_name)] - wq_tests_data['%s_nexrad_summary_24' % (clean_var_bndry_name)]

      if wq_tests_data['%s_nexrad_summary_72' % (clean_var_bndry_name)] != wq_defines.NO_DATA and\
         wq_tests_data['%s_nexrad_summary_48' % (clean_var_bndry_name)] != wq_defines.NO_DATA:
        wq_tests_data['%s_nexrad_total_2_day_delay' % (clean_var_bndry_name)] = wq_tests_data['%s_nexrad_summary_72' % (clean_var_bndry_name)] - wq_tests_data['%s_nexrad_summary_48' % (clean_var_bndry_name)]

      if wq_tests_data['%s_nexrad_summary_96' % (clean_var_bndry_name)] != wq_defines.NO_DATA and\
         wq_tests_data['%s_nexrad_summary_72' % (clean_var_bndry_name)] != wq_defines.NO_DATA:
        wq_tests_data['%s_nexrad_total_3_day_delay' % (clean_var_bndry_name)] = wq_tests_data['%s_nexrad_summary_96' % (clean_var_bndry_name)] - wq_tests_data['%s_nexrad_summary_72' % (clean_var_bndry_name)]

      prev_dry_days = self.nexrad_db.getPrecedingRadarDryDaysCount(platform_handle,
                                             start_date,
                                             'precipitation_radar_weighted_average',
                                             'mm')
      if prev_dry_days is not None:
        var_name = '%s_nexrad_dry_days_count' % (clean_var_bndry_name)
        wq_tests_data[var_name] = prev_dry_days

      rainfall_intensity = self.nexrad_db.calcRadarRainfallIntensity(platform_handle,
                                                               start_date,
                                                               60,
                                                              'precipitation_radar_weighted_average',
                                                              'mm')
      if rainfall_intensity is not None:
        var_name = '%s_nexrad_rainfall_intensity' % (clean_var_bndry_name)
        wq_tests_data[var_name] = rainfall_intensity


      if self.logger:
        self.logger.debug("Finished retrieving nexrad platfrom: %s" % (platform_handle))

    if self.logger:
      self.logger.debug("Finished retrieving nexrad data datetime: %s" % (start_date.strftime('%Y-%m-%d %H:%M:%S')))

  def get_platform_data(self, start_date, platform_handle, wq_tests_data, var_name):
      self.logger.debug("Start retrieving platform: %s datetime: %s" % (platform_handle, start_date.strftime('%Y-%m-%d %H:%M:%S')))
      salinity_id = water_temp_id = wind_spd_sensor_id = wind_dir_sensor_id = chl_sensor_id = do_percent_sensor_id = do_mg_sensor_id = None

      salinity_var_name = "%s_salinity" % var_name
      wind_spd_var_name = "%s_wind_speed" % var_name
      wind_dir_var_name = "%s_wind_dir_val" % var_name
      wind_spd_var_name_u = "%s_wind_speed_u" % var_name
      wind_dir_var_name_v = "%s_wind_dir_val_v" % var_name
      water_temp_var_name = "%s_water_temp" % var_name
      chl_var_name = "%s_chlorophyl" % var_name
      do_percent_var_name = "%s_do_percent" % var_name
      do_mg_var_name = "%s_do_mg" % var_name
      if salinity_var_name in wq_tests_data:
        #Get the sensor id for salinity
        salinity_id = self.xenia_obs_db.sensorExists('salinity', 'psu', platform_handle, 1)

      if water_temp_var_name in wq_tests_data:
        #Water temp id.
        water_temp_id = self.xenia_obs_db.sensorExists('water_temperature', 'celsius', platform_handle, 1)

      if wind_spd_var_name in wq_tests_data and wind_dir_var_name in wq_tests_data:
        #Get the sensor id for wind speed and wind direction
        wind_spd_sensor_id = self.xenia_obs_db.sensorExists('wind_speed', 'm_s-1', platform_handle, 1)
        wind_dir_sensor_id = self.xenia_obs_db.sensorExists('wind_from_direction', 'degrees_true', platform_handle, 1)

      if chl_var_name in wq_tests_data:
        chl_sensor_id = self.xenia_obs_db.sensorExists('chl_concentration', 'ug_L-1', platform_handle, 1)

      if do_percent_var_name in wq_tests_data:
        do_percent_sensor_id = self.xenia_obs_db.sensorExists('oxygen_concentration', "%", platform_handle, 1)

      if do_mg_var_name in wq_tests_data:
        do_mg_sensor_id = self.xenia_obs_db.sensorExists('oxygen_concentration', 'mg_L-1', platform_handle, 1)

      end_date = start_date
      begin_date = start_date - timedelta(hours=24)
      try:
        if salinity_id is not None:
          #(qc_level = %d OR qc_level IS NULL)
          salinity_data = self.xenia_obs_db.session.query(sl_multi_obs)\
            .filter(sl_multi_obs.m_date >= begin_date.strftime('%Y-%m-%dT%H:%M:%S'))\
            .filter(sl_multi_obs.m_date < end_date.strftime('%Y-%m-%dT%H:%M:%S'))\
            .filter(sl_multi_obs.sensor_id == salinity_id)\
            .filter(or_(sl_multi_obs.qc_level == qaqcTestFlags.DATA_QUAL_GOOD, sl_multi_obs.qc_level == None))\
            .order_by(sl_multi_obs.m_date).all()

        if water_temp_id is not None:
          water_temp_data  = self.xenia_obs_db.session.query(sl_multi_obs)\
            .filter(sl_multi_obs.m_date >= begin_date.strftime('%Y-%m-%dT%H:%M:%S'))\
            .filter(sl_multi_obs.m_date < end_date.strftime('%Y-%m-%dT%H:%M:%S'))\
            .filter(sl_multi_obs.sensor_id == water_temp_id)\
            .filter(or_(sl_multi_obs.qc_level == qaqcTestFlags.DATA_QUAL_GOOD, sl_multi_obs.qc_level == None))\
            .order_by(sl_multi_obs.m_date).all()

        if wind_spd_sensor_id is not None and wind_dir_sensor_id is not None:
          wind_speed_data = self.xenia_obs_db.session.query(sl_multi_obs)\
            .filter(sl_multi_obs.m_date >= begin_date.strftime('%Y-%m-%dT%H:%M:%S'))\
            .filter(sl_multi_obs.m_date < end_date.strftime('%Y-%m-%dT%H:%M:%S'))\
            .filter(sl_multi_obs.sensor_id == wind_spd_sensor_id)\
            .order_by(sl_multi_obs.m_date).all()

          wind_dir_data = self.xenia_obs_db.session.query(sl_multi_obs)\
            .filter(sl_multi_obs.m_date >= begin_date.strftime('%Y-%m-%dT%H:%M:%S'))\
            .filter(sl_multi_obs.m_date < end_date.strftime('%Y-%m-%dT%H:%M:%S'))\
            .filter(sl_multi_obs.sensor_id == wind_dir_sensor_id)\
            .order_by(sl_multi_obs.m_date).all()

        if chl_sensor_id is not None:
          chl_data = self.xenia_obs_db.session.query(sl_multi_obs)\
            .filter(sl_multi_obs.m_date >= begin_date.strftime('%Y-%m-%dT%H:%M:%S'))\
            .filter(sl_multi_obs.m_date < end_date.strftime('%Y-%m-%dT%H:%M:%S'))\
            .filter(sl_multi_obs.sensor_id == chl_sensor_id)\
            .order_by(sl_multi_obs.m_date).all()

        if do_percent_sensor_id is not None:
          do_percent_data = self.xenia_obs_db.session.query(sl_multi_obs)\
            .filter(sl_multi_obs.m_date >= begin_date.strftime('%Y-%m-%dT%H:%M:%S'))\
            .filter(sl_multi_obs.m_date < end_date.strftime('%Y-%m-%dT%H:%M:%S'))\
            .filter(sl_multi_obs.sensor_id == do_percent_sensor_id)\
            .order_by(sl_multi_obs.m_date).all()

        if do_mg_sensor_id is not None:
          do_mg_data = self.xenia_obs_db.session.query(sl_multi_obs)\
            .filter(sl_multi_obs.m_date >= begin_date.strftime('%Y-%m-%dT%H:%M:%S'))\
            .filter(sl_multi_obs.m_date < end_date.strftime('%Y-%m-%dT%H:%M:%S'))\
            .filter(sl_multi_obs.sensor_id == do_mg_sensor_id)\
            .order_by(sl_multi_obs.m_date).all()

      except Exception as e:
        self.logger.exception(e)
      else:
        if salinity_id is not None:
          if len(salinity_data):
            wq_tests_data[salinity_var_name] =sum(sal.m_value for sal in salinity_data) / len(salinity_data)
          self.logger.debug("Platform: %s Avg Salinity: %f Records used: %d" % (platform_handle,wq_tests_data[salinity_var_name], len(salinity_data)))
        if water_temp_id is not None:
          if len(water_temp_data):
            wq_tests_data[water_temp_var_name] = sum(temp.m_value for temp in water_temp_data) / len(water_temp_data)
          self.logger.debug("Platform: %s Avg Water Temp: %f Records used: %d" % (platform_handle,wq_tests_data[water_temp_var_name], len(water_temp_data)))

        if chl_sensor_id is not None:
          if len(chl_data):
            wq_tests_data[chl_var_name] = sum(rec.m_value for rec in chl_data) / len(chl_data)
          self.logger.debug("Platform: %s Avg Chl: %f Records used: %d" % (platform_handle,wq_tests_data[chl_var_name], len(chl_data)))

        if do_percent_sensor_id is not None:
          if len(do_percent_data):
            wq_tests_data[do_percent_var_name] = sum(rec.m_value for rec in do_percent_data) / len(do_percent_data)
          self.logger.debug("Platform: %s Avg DO %%: %f Records used: %d" % (platform_handle, wq_tests_data[do_percent_var_name], len(do_percent_data)))

        if do_mg_sensor_id is not None:
          if len(do_mg_data):
            wq_tests_data[do_mg_var_name] = sum(rec.m_value for rec in do_mg_data) / len(do_mg_data)
          self.logger.debug("Platform: %s Avg Do mg: %f Records used: %d" % (platform_handle,wq_tests_data[do_mg_var_name], len(do_mg_data)))

        if wind_dir_sensor_id is not None and wind_spd_sensor_id is not None:
          wind_dir_tuples = []
          direction_tuples = []
          scalar_speed_avg = None
          speed_count = 0
          for wind_speed_row in wind_speed_data:
            for wind_dir_row in wind_dir_data:
              if wind_speed_row.m_date == wind_dir_row.m_date:
                self.logger.debug("Building tuple for Speed(%s): %f Dir(%s): %f" % (wind_speed_row.m_date, wind_speed_row.m_value, wind_dir_row.m_date, wind_dir_row.m_value))
                if scalar_speed_avg is None:
                  scalar_speed_avg = 0
                scalar_speed_avg += wind_speed_row.m_value
                speed_count += 1
                #Vector using both speed and direction.
                wind_dir_tuples.append((wind_speed_row.m_value, wind_dir_row.m_value))
                #Vector with speed as constant(1), and direction.
                direction_tuples.append((1, wind_dir_row.m_value))
                break

          if len(wind_dir_tuples):
            avg_speed_dir_components = calcAvgSpeedAndDir(wind_dir_tuples)
            self.logger.debug("Platform: %s Avg Wind Speed: %f(m_s-1) %f(mph) Direction: %f" % (platform_handle,
                                                                                              avg_speed_dir_components[0],
                                                                                              avg_speed_dir_components[0] * meters_per_second_to_mph,
                                                                                              avg_speed_dir_components[1]))

            #Unity components, just direction with speeds all 1.
            avg_dir_components = calcAvgSpeedAndDirV2(direction_tuples)
            scalar_speed_avg = scalar_speed_avg / speed_count
            wq_tests_data[wind_spd_var_name] = scalar_speed_avg * meters_per_second_to_mph
            wq_tests_data[wind_dir_var_name] = avg_dir_components['scalar'][1]
            wq_tests_data[wind_spd_var_name_u] = avg_dir_components['vector'][0]
            wq_tests_data[wind_dir_var_name_v] = avg_dir_components['vector'][1]

            self.logger.debug("Platform: %s Avg Scalar Wind Speed: %f(m_s-1) %f(mph) Direction: %f" % (platform_handle,
                                                                                                     scalar_speed_avg,
                                                                                                     scalar_speed_avg * meters_per_second_to_mph,
                                                                                                     avg_dir_components['scalar'][1]))
        else:
          self.logger.debug("Platform: %s no wind data found for datetime: %s" % (platform_handle, start_date.strftime('%Y-%m-%d %H:%M:%S')))

      self.logger.debug("Finished retrieving platform: %s datetime: %s" % (platform_handle, start_date.strftime('%Y-%m-%d %H:%M:%S')))


  def get_sun2_data(self, start_date, wq_tests_data):
      platform_handle = 'carocoops.SUN2.buoy'

      self.logger.debug("Start retrieving platform: %s datetime: %s" % (platform_handle, start_date.strftime('%Y-%m-%d %H:%M:%S')))

      #Get the sensor id for salinity
      salinity_id = self.xenia_obs_db.sensorExists('salinity', 'psu', platform_handle, 1)

      #Water temp id.
      water_temp_id = self.xenia_obs_db.sensorExists('water_temperature', 'celsius', platform_handle, 1)

      #Get the sensor id for wind speed and wind direction
      wind_spd_sensor_id = self.xenia_obs_db.sensorExists('wind_speed', 'm_s-1', platform_handle, 1)
      wind_dir_sensor_id = self.xenia_obs_db.sensorExists('wind_from_direction', 'degrees_true', platform_handle, 1)

      end_date = start_date
      begin_date = start_date - timedelta(hours=24)
      try:
        #(qc_level = %d OR qc_level IS NULL)
        salinity_data = self.xenia_obs_db.session.query(sl_multi_obs)\
          .filter(sl_multi_obs.m_date >= begin_date.strftime('%Y-%m-%dT%H:%M:%S'))\
          .filter(sl_multi_obs.m_date < end_date.strftime('%Y-%m-%dT%H:%M:%S'))\
          .filter(sl_multi_obs.sensor_id == salinity_id)\
          .filter(or_(sl_multi_obs.qc_level == qaqcTestFlags.DATA_QUAL_GOOD, sl_multi_obs.qc_level == None))\
          .order_by(sl_multi_obs.m_date).all()

        water_temp_data  = self.xenia_obs_db.session.query(sl_multi_obs)\
          .filter(sl_multi_obs.m_date >= begin_date.strftime('%Y-%m-%dT%H:%M:%S'))\
          .filter(sl_multi_obs.m_date < end_date.strftime('%Y-%m-%dT%H:%M:%S'))\
          .filter(sl_multi_obs.sensor_id == water_temp_id)\
          .filter(or_(sl_multi_obs.qc_level == qaqcTestFlags.DATA_QUAL_GOOD, sl_multi_obs.qc_level == None))\
          .order_by(sl_multi_obs.m_date).all()

        wind_speed_data = self.xenia_obs_db.session.query(sl_multi_obs)\
          .filter(sl_multi_obs.m_date >= begin_date.strftime('%Y-%m-%dT%H:%M:%S'))\
          .filter(sl_multi_obs.m_date < end_date.strftime('%Y-%m-%dT%H:%M:%S'))\
          .filter(sl_multi_obs.sensor_id == wind_spd_sensor_id)\
          .order_by(sl_multi_obs.m_date).all()

        wind_dir_data = self.xenia_obs_db.session.query(sl_multi_obs)\
          .filter(sl_multi_obs.m_date >= begin_date.strftime('%Y-%m-%dT%H:%M:%S'))\
          .filter(sl_multi_obs.m_date < end_date.strftime('%Y-%m-%dT%H:%M:%S'))\
          .filter(sl_multi_obs.sensor_id == wind_dir_sensor_id)\
          .order_by(sl_multi_obs.m_date).all()
      except Exception as e:
        self.logger.exception(e)
      else:
        if len(salinity_data):
          wq_tests_data['sun2_salinity'] = sum(sal.m_value for sal in salinity_data) / len(salinity_data)
        self.logger.debug("Platform: %s Avg Salinity: %f Records used: %d" % (platform_handle,wq_tests_data['sun2_salinity'], len(salinity_data)))

        if len(water_temp_data):
          wq_tests_data['sun2_water_temp'] = sum(temp.m_value for temp in water_temp_data) / len(water_temp_data)
        self.logger.debug("Platform: %s Avg Water Temp: %f Records used: %d" % (platform_handle,wq_tests_data['sun2_water_temp'], len(water_temp_data)))

        wind_dir_tuples = []
        direction_tuples = []
        scalar_speed_avg = None
        speed_count = 0
        for wind_speed_row in wind_speed_data:
          for wind_dir_row in wind_dir_data:
            if wind_speed_row.m_date == wind_dir_row.m_date:
              self.logger.debug("Building tuple for Speed(%s): %f Dir(%s): %f" % (wind_speed_row.m_date, wind_speed_row.m_value, wind_dir_row.m_date, wind_dir_row.m_value))
              if scalar_speed_avg is None:
                scalar_speed_avg = 0
              scalar_speed_avg += wind_speed_row.m_value
              speed_count += 1
              #Vector using both speed and direction.
              wind_dir_tuples.append((wind_speed_row.m_value, wind_dir_row.m_value))
              #Vector with speed as constant(1), and direction.
              direction_tuples.append((1, wind_dir_row.m_value))
              break

        avg_speed_dir_components = calcAvgSpeedAndDir(wind_dir_tuples)
        self.logger.debug("Platform: %s Avg Wind Speed: %f(m_s-1) %f(mph) Direction: %f" % (platform_handle,
                                                                                          avg_speed_dir_components[0],
                                                                                          avg_speed_dir_components[0] * meters_per_second_to_mph,
                                                                                          avg_speed_dir_components[1]))

        #Unity components, just direction with speeds all 1.
        avg_dir_components = calcAvgSpeedAndDir(direction_tuples)
        scalar_speed_avg = scalar_speed_avg / speed_count
        wq_tests_data['sun2_wind_speed'] = scalar_speed_avg * meters_per_second_to_mph
        wq_tests_data['sun2_wind_dir_val'] = avg_dir_components[1]
        self.logger.debug("Platform: %s Avg Scalar Wind Speed: %f(m_s-1) %f(mph) Direction: %f" % (platform_handle,
                                                                                                 scalar_speed_avg,
                                                                                                 scalar_speed_avg * meters_per_second_to_mph,
                                                                                                 avg_dir_components[1]))

      self.logger.debug("Finished retrieving platform: %s datetime: %s" % (platform_handle, start_date.strftime('%Y-%m-%d %H:%M:%S')))

      return

  def get_nos_data(self, start_date, wq_tests_data):
      platform_handle = 'nos.8661070.WL'

      self.logger.debug("Start retrieving platform: %s datetime: %s" % (platform_handle, start_date.strftime('%Y-%m-%d %H:%M:%S')))

      #Water temp id.
      water_temp_id = self.xenia_obs_db.sensorExists('water_temperature', 'celsius', platform_handle, 1)

      water_level_id = self.xenia_obs_db.sensorExists('water_level', 'm', platform_handle, 1)
      #Get the sensor id for wind speed and wind direction
      wind_spd_sensor_id = self.xenia_obs_db.sensorExists('wind_speed', 'm_s-1', platform_handle, 1)
      wind_dir_sensor_id = self.xenia_obs_db.sensorExists('wind_from_direction', 'degrees_true', platform_handle, 1)

      end_date = start_date
      begin_date = start_date - timedelta(hours=24)
      try:
        water_temp_data  = self.xenia_obs_db.session.query(sl_multi_obs)\
          .filter(sl_multi_obs.m_date >= begin_date.strftime('%Y-%m-%dT%H:%M:%S'))\
          .filter(sl_multi_obs.m_date < end_date.strftime('%Y-%m-%dT%H:%M:%S'))\
          .filter(sl_multi_obs.sensor_id == water_temp_id)\
          .order_by(sl_multi_obs.m_date).all()

        water_level_data  = self.xenia_obs_db.session.query(sl_multi_obs)\
          .filter(sl_multi_obs.m_date >= begin_date.strftime('%Y-%m-%dT%H:%M:%S'))\
          .filter(sl_multi_obs.m_date < end_date.strftime('%Y-%m-%dT%H:%M:%S'))\
          .filter(sl_multi_obs.sensor_id == water_level_id)\
          .order_by(sl_multi_obs.m_date).all()


        wind_speed_data = self.xenia_obs_db.session.query(sl_multi_obs)\
          .filter(sl_multi_obs.m_date >= begin_date.strftime('%Y-%m-%dT%H:%M:%S'))\
          .filter(sl_multi_obs.m_date < end_date.strftime('%Y-%m-%dT%H:%M:%S'))\
          .filter(sl_multi_obs.sensor_id == wind_spd_sensor_id)\
          .order_by(sl_multi_obs.m_date).all()

        wind_dir_data = self.xenia_obs_db.session.query(sl_multi_obs)\
          .filter(sl_multi_obs.m_date >= begin_date.strftime('%Y-%m-%dT%H:%M:%S'))\
          .filter(sl_multi_obs.m_date < end_date.strftime('%Y-%m-%dT%H:%M:%S'))\
          .filter(sl_multi_obs.sensor_id == wind_dir_sensor_id)\
          .order_by(sl_multi_obs.m_date).all()
      except Exception as e:
        self.logger.exception(e)
      else:
        try:
          if len(water_temp_data):
            wq_tests_data['nos8661070_water_temp'] = sum(rec.m_value for rec in water_temp_data) / len(water_temp_data)
          self.logger.debug("Platform: %s Avg Water Temp: %f Records used: %d" % (platform_handle,wq_tests_data['nos8661070_water_temp'], len(water_temp_data)))

          if len(water_level_data):
            wq_tests_data['nos8661070_water_level'] = sum(rec.m_value for rec in water_level_data) / len(water_level_data)
          self.logger.debug("Platform: %s Avg Water Level: %f Records used: %d" % (platform_handle,wq_tests_data['nos8661070_water_level'], len(water_level_data)))

          wind_dir_tuples = []
          direction_tuples = []
          scalar_speed_avg = None
          speed_count = 0
          for wind_speed_row in wind_speed_data:
            for wind_dir_row in wind_dir_data:
              if wind_speed_row.m_date == wind_dir_row.m_date:
                self.logger.debug("Building tuple for Speed(%s): %f Dir(%s): %f" % (wind_speed_row.m_date, wind_speed_row.m_value, wind_dir_row.m_date, wind_dir_row.m_value))
                if scalar_speed_avg is None:
                  scalar_speed_avg = 0
                scalar_speed_avg += wind_speed_row.m_value
                speed_count += 1
                #Vector using both speed and direction.
                wind_dir_tuples.append((wind_speed_row.m_value, wind_dir_row.m_value))
                #Vector with speed as constant(1), and direction.
                direction_tuples.append((1, wind_dir_row.m_value))
                break
          if len(wind_dir_tuples):
            avg_speed_dir_components = calcAvgSpeedAndDirV2(wind_dir_tuples)
            self.logger.debug("Platform: %s Avg Wind Speed: %f(m_s-1) %f(mph) Direction: %f" % (platform_handle,
                                                                                                avg_speed_dir_components['scalar'][0],
                                                                                                avg_speed_dir_components['scalar'][0] * meters_per_second_to_mph,
                                                                                                avg_speed_dir_components['scalar'][1]))

            #Unity components, just direction with speeds all 1.
            avg_dir_components = calcAvgSpeedAndDirV2(direction_tuples)
            scalar_speed_avg = scalar_speed_avg / speed_count
            wq_tests_data['nos8661070_wind_spd'] = scalar_speed_avg * meters_per_second_to_mph
            wq_tests_data['nos8661070_wind_dir_val'] = avg_dir_components['scalar'][1]
            self.logger.debug("Platform: %s Avg Scalar Wind Speed: %f(m_s-1) %f(mph) Direction: %f" % (platform_handle,
                                                                                                       scalar_speed_avg,
                                                                                                       scalar_speed_avg * meters_per_second_to_mph,
                                                                                                       avg_dir_components['scalar'][1]))
        except Exception as e:
          self.logger.exception(e)
      self.logger.debug("Finished retrieving platform: %s datetime: %s" % (platform_handle, start_date.strftime('%Y-%m-%d %H:%M:%S')))

      return

class mb_wq_model_data(wq_data):
  def __init__(self, **kwargs):
    wq_data.__init__(self, **kwargs)

    self.logger = logging.getLogger(type(self).__name__)

    #List of platforms that we query same obs types from
    self.platforms = ['carocoops.SUN2.buoy', 'lbhmc.2ndave.pier', 'lbhmc.cherrygrove.pier', 'lbhmc.apachepier.pier']

    self.site = None
    #The main station we retrieve the values from.
    self.tide_station =  None
    self.query_tide_data = True
    self.tide_data_obj = None

    self.logger.debug("Connection to xenia wq db: %s" % (kwargs['xenia_wq_db_name']))
    self.xenia_wq_db = wqDB(kwargs['xenia_wq_db_name'], type(self).__name__)

    try:
      #Connect to the xenia database we use for observations aggregation.
      self.xenia_obs_db = xeniaAlchemy()
      if self.xenia_obs_db.connectDB(kwargs['xenia_obs_db_type'], kwargs['xenia_obs_db_user'], kwargs['xenia_obs_db_password'], kwargs['xenia_obs_db_host'], kwargs['xenia_obs_db_name'], False):
        self.logger.info("Succesfully connect to DB: %s at %s" %(kwargs['xenia_obs_db_name'],kwargs['xenia_obs_db_host']))
      else:
        self.logger.error("Unable to connect to DB: %s at %s." %(kwargs['xenia_obs_db_name'],kwargs['xenia_obs_db_host']))


    except Exception as e:
      self.logger.exception(e)
      raise

  def __del__(self):
    if self.logger:
      self.logger.debug("Disconnecting xenia obs database.")
    self.xenia_obs_db.disconnect()

  def reset(self, **kwargs):
    self.site = kwargs['site']
    self.tide_station = kwargs['tide_station']

  """
  Function: initialize_return_data
  Purpose: INitialize our ordered dict with the data variables and assign a NO_DATA
    initial value.
  Parameters:
    wq_tests_data - An OrderedDict that is initialized.
  Return:
    None
  """
  def initialize_return_data(self, wq_tests_data, initialize_site_specific_data_only):
    if self.logger:
      self.logger.debug("Creating and initializing data dict.")

    if not initialize_site_specific_data_only:
      wq_tests_data['nos8661070_wind_spd'] = wq_defines.NO_DATA
      wq_tests_data['nos8661070_wind_dir_val'] = wq_defines.NO_DATA
      wq_tests_data['nos8661070_water_temp'] = wq_defines.NO_DATA
      wq_tests_data['nos8661070_water_level'] = wq_defines.NO_DATA

      for platform in self.platforms:
        platform_name = platform.split('.')

        #Sympify does not like expression variables that start with a number.
        if platform_name[1] == '2ndave':
          platform_name[1] = 'secondavenorth'
        var_name = '%s_water_temp' % (platform_name[1].lower())
        wq_tests_data[var_name] = wq_defines.NO_DATA
        var_name = '%s_salinity' % (platform_name[1].lower())
        wq_tests_data[var_name] = wq_defines.NO_DATA
        if platform == 'carocoops.SUN2.buoy':
          var_name = '%s_wind_speed' % (platform_name[1].lower())
          wq_tests_data[var_name] = wq_defines.NO_DATA
          var_name = '%s_wind_dir_val' % (platform_name[1].lower())
          wq_tests_data[var_name] = wq_defines.NO_DATA
        else:
          var_name = "%s_chlorophyl" % (platform_name[1].lower())
          wq_tests_data[var_name] = wq_defines.NO_DATA
          var_name = "%s_do_percent" % (platform_name[1].lower())
          wq_tests_data[var_name] = wq_defines.NO_DATA
          var_name = "%s_do_mg" % (platform_name[1].lower())
          wq_tests_data[var_name] = wq_defines.NO_DATA


      #Build variables for the base tide station.
      var_name = 'tide_range_%s' % (self.tide_station)
      wq_tests_data[var_name] = wq_defines.NO_DATA
      var_name = 'tide_hi_%s' % (self.tide_station)
      wq_tests_data[var_name] = wq_defines.NO_DATA
      var_name = 'tide_lo_%s' % (self.tide_station)
      wq_tests_data[var_name] = wq_defines.NO_DATA


    for boundary in self.site.contained_by:
      if len(boundary.name):
        for prev_hours in range(24, 192, 24):
          clean_var_boundary_name = boundary.name.lower().replace(' ', '_')
          var_name = '%s_nexrad_summary_%d' % (clean_var_boundary_name, prev_hours)
          wq_tests_data[var_name] = wq_defines.NO_DATA

        var_name = '%s_nexrad_dry_days_count' % (clean_var_boundary_name)
        wq_tests_data[var_name] = wq_defines.NO_DATA

        var_name = '%s_nexrad_rainfall_intensity' % (clean_var_boundary_name)
        wq_tests_data[var_name] = wq_defines.NO_DATA

        var_name = '%s_nexrad_total_1_day_delay' % (clean_var_boundary_name)
        wq_tests_data[var_name] = wq_defines.NO_DATA
        var_name = '%s_nexrad_total_2_day_delay' % (clean_var_boundary_name)
        wq_tests_data[var_name] = wq_defines.NO_DATA
        var_name = '%s_nexrad_total_3_day_delay' % (clean_var_boundary_name)
        wq_tests_data[var_name] = wq_defines.NO_DATA


    self.logger.debug("Finished creating and initializing data dict.")

    return

  """
  Function: query_data
  Purpose: Retrieves all the data used in the modelling project.
  Parameters:
    start_data - Datetime object representing the starting date to query data for.
    end_date - Datetime object representing the ending date to query data for.
    wq_tests_data - A OrderedDict object where the retrieved data is store.
  Return:
    None
  """
  def query_data(self, start_date, end_date, wq_tests_data, reset_site_specific_data_only):
    self.logger.debug("Site: %s start query data for datetime: %s" % (self.site.name, start_date))

    self.initialize_return_data(wq_tests_data, reset_site_specific_data_only)

    #If we are resetting only the site specific data, no need to re-query these.
    if not reset_site_specific_data_only:
      self.get_tide_data(start_date, wq_tests_data)
      #self.get_nos_data(start_date, wq_tests_data)
      for platform in self.platforms:
        var_name = platform.split('.')
        if var_name[1] == '2ndave':
          var_name[1] = 'secondavenorth'
        self.get_platform_data(start_date, platform, wq_tests_data, var_name[1].lower())
      """
      self.get_sun2_data(start_date, wq_tests_data)
      self.get_2ndave_data(start_date, wq_tests_data)
      self.get_apache_data(start_date, wq_tests_data)
      self.get_cherrygrove_data(start_date, wq_tests_data)
      """
    self.get_nexrad_data(start_date, wq_tests_data)

    self.logger.debug("Site: %s Finished query data for datetime: %s" % (self.site.name, start_date))

  def get_platform_data(self, start_date, platform_handle, wq_tests_data, var_name):
      self.logger.debug("Start retrieving platform: %s datetime: %s" % (platform_handle, start_date.strftime('%Y-%m-%d %H:%M:%S')))
      salinity_id = water_temp_id = wind_spd_sensor_id = wind_dir_sensor_id = chl_sensor_id = do_percent_sensor_id = do_mg_sensor_id = None

      salinity_var_name = "%s_salinity" % var_name
      wind_spd_var_name = "%s_wind_speed" % var_name
      wind_dir_var_name = "%s_wind_dir_val" % var_name
      water_temp_var_name = "%s_water_temp" % var_name
      chl_var_name = "%s_chlorophyl" % var_name
      do_percent_var_name = "%s_do_percent" % var_name
      do_mg_var_name = "%s_do_mg" % var_name
      if salinity_var_name in wq_tests_data:
        #Get the sensor id for salinity
        salinity_id = self.xenia_obs_db.sensorExists('salinity', 'psu', platform_handle, 1)

      if water_temp_var_name in wq_tests_data:
        #Water temp id.
        water_temp_id = self.xenia_obs_db.sensorExists('water_temperature', 'celsius', platform_handle, 1)

      if wind_spd_var_name in wq_tests_data and wind_dir_var_name in wq_tests_data:
        #Get the sensor id for wind speed and wind direction
        wind_spd_sensor_id = self.xenia_obs_db.sensorExists('wind_speed', 'm_s-1', platform_handle, 1)
        wind_dir_sensor_id = self.xenia_obs_db.sensorExists('wind_from_direction', 'degrees_true', platform_handle, 1)

      if chl_var_name in wq_tests_data:
        chl_sensor_id = self.xenia_obs_db.sensorExists('chl_concentration', 'ug_L-1', platform_handle, 1)

      if do_percent_var_name in wq_tests_data:
        do_percent_sensor_id = self.xenia_obs_db.sensorExists('oxygen_concentration', "percent", platform_handle, 1)

      if do_mg_var_name in wq_tests_data:
        do_mg_sensor_id = self.xenia_obs_db.sensorExists('oxygen_concentration', 'mg_L-1', platform_handle, 1)

      end_date = start_date
      begin_date = start_date - timedelta(hours=24)
      try:
        if salinity_id is not None:
          #(qc_level = %d OR qc_level IS NULL)
          salinity_data = self.xenia_obs_db.session.query(sl_multi_obs)\
            .filter(sl_multi_obs.m_date >= begin_date.strftime('%Y-%m-%dT%H:%M:%S'))\
            .filter(sl_multi_obs.m_date < end_date.strftime('%Y-%m-%dT%H:%M:%S'))\
            .filter(sl_multi_obs.sensor_id == salinity_id)\
            .filter(or_(sl_multi_obs.qc_level == qaqcTestFlags.DATA_QUAL_GOOD, sl_multi_obs.qc_level == None))\
            .order_by(sl_multi_obs.m_date).all()

        if water_temp_id is not None:
          water_temp_data  = self.xenia_obs_db.session.query(sl_multi_obs)\
            .filter(sl_multi_obs.m_date >= begin_date.strftime('%Y-%m-%dT%H:%M:%S'))\
            .filter(sl_multi_obs.m_date < end_date.strftime('%Y-%m-%dT%H:%M:%S'))\
            .filter(sl_multi_obs.sensor_id == water_temp_id)\
            .filter(or_(sl_multi_obs.qc_level == qaqcTestFlags.DATA_QUAL_GOOD, sl_multi_obs.qc_level == None))\
            .order_by(sl_multi_obs.m_date).all()

        if wind_spd_sensor_id is not None and wind_dir_sensor_id is not None:
          wind_speed_data = self.xenia_obs_db.session.query(sl_multi_obs)\
            .filter(sl_multi_obs.m_date >= begin_date.strftime('%Y-%m-%dT%H:%M:%S'))\
            .filter(sl_multi_obs.m_date < end_date.strftime('%Y-%m-%dT%H:%M:%S'))\
            .filter(sl_multi_obs.sensor_id == wind_spd_sensor_id)\
            .order_by(sl_multi_obs.m_date).all()

          wind_dir_data = self.xenia_obs_db.session.query(sl_multi_obs)\
            .filter(sl_multi_obs.m_date >= begin_date.strftime('%Y-%m-%dT%H:%M:%S'))\
            .filter(sl_multi_obs.m_date < end_date.strftime('%Y-%m-%dT%H:%M:%S'))\
            .filter(sl_multi_obs.sensor_id == wind_dir_sensor_id)\
            .order_by(sl_multi_obs.m_date).all()

        if chl_sensor_id is not None:
          chl_data = self.xenia_obs_db.session.query(sl_multi_obs)\
            .filter(sl_multi_obs.m_date >= begin_date.strftime('%Y-%m-%dT%H:%M:%S'))\
            .filter(sl_multi_obs.m_date < end_date.strftime('%Y-%m-%dT%H:%M:%S'))\
            .filter(sl_multi_obs.sensor_id == chl_sensor_id)\
            .order_by(sl_multi_obs.m_date).all()

        if do_percent_sensor_id is not None:
          do_percent_data = self.xenia_obs_db.session.query(sl_multi_obs)\
            .filter(sl_multi_obs.m_date >= begin_date.strftime('%Y-%m-%dT%H:%M:%S'))\
            .filter(sl_multi_obs.m_date < end_date.strftime('%Y-%m-%dT%H:%M:%S'))\
            .filter(sl_multi_obs.sensor_id == do_percent_sensor_id)\
            .order_by(sl_multi_obs.m_date).all()

        if do_mg_sensor_id is not None:
          do_mg_data = self.xenia_obs_db.session.query(sl_multi_obs)\
            .filter(sl_multi_obs.m_date >= begin_date.strftime('%Y-%m-%dT%H:%M:%S'))\
            .filter(sl_multi_obs.m_date < end_date.strftime('%Y-%m-%dT%H:%M:%S'))\
            .filter(sl_multi_obs.sensor_id == do_mg_sensor_id)\
            .order_by(sl_multi_obs.m_date).all()

      except Exception as e:
        self.logger.exception(e)
      else:
        if salinity_id is not None:
          if len(salinity_data):
            wq_tests_data[salinity_var_name] =sum(sal.m_value for sal in salinity_data) / len(salinity_data)
          self.logger.debug("Platform: %s Avg Salinity: %f Records used: %d" % (platform_handle,wq_tests_data[salinity_var_name], len(salinity_data)))
        if water_temp_id is not None:
          if len(water_temp_data):
            wq_tests_data[water_temp_var_name] = sum(temp.m_value for temp in water_temp_data) / len(water_temp_data)
          self.logger.debug("Platform: %s Avg Water Temp: %f Records used: %d" % (platform_handle,wq_tests_data[water_temp_var_name], len(water_temp_data)))

        if chl_sensor_id is not None:
          if len(chl_data):
            wq_tests_data[chl_var_name] = sum(rec.m_value for rec in chl_data) / len(chl_data)
          self.logger.debug("Platform: %s Avg Chl: %f Records used: %d" % (platform_handle,wq_tests_data[chl_var_name], len(chl_data)))

        if do_percent_sensor_id is not None:
          if len(do_percent_data):
            wq_tests_data[do_percent_var_name] = sum(rec.m_value for rec in do_percent_data) / len(do_percent_data)
          self.logger.debug("Platform: %s Avg DO %%: %f Records used: %d" % (platform_handle, wq_tests_data[do_percent_var_name], len(do_percent_data)))

        if do_mg_sensor_id is not None:
          if len(do_mg_data):
            wq_tests_data[do_mg_var_name] = sum(rec.m_value for rec in do_mg_data) / len(do_mg_data)
          self.logger.debug("Platform: %s Avg Do mg: %f Records used: %d" % (platform_handle,wq_tests_data[do_mg_var_name], len(do_mg_data)))

        if wind_dir_sensor_id is not None and wind_spd_sensor_id is not None:
          wind_dir_tuples = []
          direction_tuples = []
          scalar_speed_avg = None
          speed_count = 0
          for wind_speed_row in wind_speed_data:
            for wind_dir_row in wind_dir_data:
              if wind_speed_row.m_date == wind_dir_row.m_date:
                self.logger.debug("Building tuple for Speed(%s): %f Dir(%s): %f" % (wind_speed_row.m_date, wind_speed_row.m_value, wind_dir_row.m_date, wind_dir_row.m_value))
                if scalar_speed_avg is None:
                  scalar_speed_avg = 0
                scalar_speed_avg += wind_speed_row.m_value
                speed_count += 1
                #Vector using both speed and direction.
                wind_dir_tuples.append((wind_speed_row.m_value, wind_dir_row.m_value))
                #Vector with speed as constant(1), and direction.
                direction_tuples.append((1, wind_dir_row.m_value))
                break

          if len(wind_dir_tuples):
            avg_speed_dir_components = calcAvgSpeedAndDirV2(wind_dir_tuples)
            self.logger.debug("Platform: %s Avg Wind Speed: %f(m_s-1) %f(mph) Direction: %f" % (platform_handle,
                                                                                              avg_speed_dir_components['scalar'][0],
                                                                                              avg_speed_dir_components['scalar'][0] * meters_per_second_to_mph,
                                                                                              avg_speed_dir_components['scalar'][1]))

            #Unity components, just direction with speeds all 1.
            avg_dir_components = calcAvgSpeedAndDirV2(direction_tuples)
            scalar_speed_avg = scalar_speed_avg / speed_count
            wq_tests_data[wind_spd_var_name] = scalar_speed_avg * meters_per_second_to_mph
            wq_tests_data[wind_dir_var_name] = avg_dir_components['scalar'][1]
            self.logger.debug("Platform: %s Avg Scalar Wind Speed: %f(m_s-1) %f(mph) Direction: %f" % (platform_handle,
                                                                                                     scalar_speed_avg,
                                                                                                     scalar_speed_avg * meters_per_second_to_mph,
                                                                                                     avg_dir_components['scalar'][1]))
        else:
          self.logger.debug("Platform: %s no wind data found for datetime: %s" % (platform_handle, start_date.strftime('%Y-%m-%d %H:%M:%S')))

      self.logger.debug("Finished retrieving platform: %s datetime: %s" % (platform_handle, start_date.strftime('%Y-%m-%d %H:%M:%S')))
  
  def get_nos_data(self, start_date, wq_tests_data):
      platform_handle = 'nos.8661070.WL'

      self.logger.debug("Start retrieving platform: %s datetime: %s" % (platform_handle, start_date.strftime('%Y-%m-%d %H:%M:%S')))

      #Water temp id.
      water_temp_id = self.xenia_obs_db.sensorExists('water_temperature', 'celsius', platform_handle, 1)

      water_level_id = self.xenia_obs_db.sensorExists('water_level', 'm', platform_handle, 1)
      #Get the sensor id for wind speed and wind direction
      wind_spd_sensor_id = self.xenia_obs_db.sensorExists('wind_speed', 'm_s-1', platform_handle, 1)
      wind_dir_sensor_id = self.xenia_obs_db.sensorExists('wind_from_direction', 'degrees_true', platform_handle, 1)

      end_date = start_date
      begin_date = start_date - timedelta(hours=24)
      try:
        water_temp_data  = self.xenia_obs_db.session.query(multi_obs)\
          .filter(multi_obs.m_date >= begin_date.strftime('%Y-%m-%dT%H:%M:%S'))\
          .filter(multi_obs.m_date < end_date.strftime('%Y-%m-%dT%H:%M:%S'))\
          .filter(multi_obs.sensor_id == water_temp_id)\
          .order_by(multi_obs.m_date).all()

        water_level_data  = self.xenia_obs_db.session.query(multi_obs)\
          .filter(multi_obs.m_date >= begin_date.strftime('%Y-%m-%dT%H:%M:%S'))\
          .filter(multi_obs.m_date < end_date.strftime('%Y-%m-%dT%H:%M:%S'))\
          .filter(multi_obs.sensor_id == water_level_id)\
          .order_by(multi_obs.m_date).all()


        wind_speed_data = self.xenia_obs_db.session.query(multi_obs)\
          .filter(multi_obs.m_date >= begin_date.strftime('%Y-%m-%dT%H:%M:%S'))\
          .filter(multi_obs.m_date < end_date.strftime('%Y-%m-%dT%H:%M:%S'))\
          .filter(multi_obs.sensor_id == wind_spd_sensor_id)\
          .order_by(multi_obs.m_date).all()

        wind_dir_data = self.xenia_obs_db.session.query(multi_obs)\
          .filter(multi_obs.m_date >= begin_date.strftime('%Y-%m-%dT%H:%M:%S'))\
          .filter(multi_obs.m_date < end_date.strftime('%Y-%m-%dT%H:%M:%S'))\
          .filter(multi_obs.sensor_id == wind_dir_sensor_id)\
          .order_by(multi_obs.m_date).all()
      except Exception as e:
        self.logger.exception(e)
      else:

        if len(water_temp_data):
          wq_tests_data['nos8661070_water_temp'] = sum(rec.m_value for rec in water_temp_data) / len(water_temp_data)
          self.logger.debug("Platform: %s Avg Water Temp: %f Records used: %d" % (platform_handle,wq_tests_data['nos8661070_water_temp'], len(water_temp_data)))
        else:
          self.logger.error("Platform: %s returned no water_temperature records." % (platform_handle))

        if len(water_level_data):
          wq_tests_data['nos8661070_water_level'] = sum(rec.m_value for rec in water_level_data) / len(water_level_data)
          self.logger.debug("Platform: %s Avg Water Level: %f Records used: %d" % (platform_handle,wq_tests_data['nos8661070_water_level'], len(water_level_data)))
        else:
          self.logger.error("Platform: %s returned no water_level records." % (platform_handle))

        wind_dir_tuples = []
        direction_tuples = []
        scalar_speed_avg = None
        speed_count = 0
        for wind_speed_row in wind_speed_data:
          for wind_dir_row in wind_dir_data:
            if wind_speed_row.m_date == wind_dir_row.m_date:
              self.logger.debug("Building tuple for Speed(%s): %f Dir(%s): %f" % (wind_speed_row.m_date, wind_speed_row.m_value, wind_dir_row.m_date, wind_dir_row.m_value))
              if scalar_speed_avg is None:
                scalar_speed_avg = 0
              scalar_speed_avg += wind_speed_row.m_value
              speed_count += 1
              #Vector using both speed and direction.
              wind_dir_tuples.append((wind_speed_row.m_value, wind_dir_row.m_value))
              #Vector with speed as constant(1), and direction.
              direction_tuples.append((1, wind_dir_row.m_value))
              break
        if len(wind_dir_tuples):
          avg_speed_dir_components = calcAvgSpeedAndDir(wind_dir_tuples)
          self.logger.debug("Platform: %s Avg Wind Speed: %f(m_s-1) %f(mph) Direction: %f" % (platform_handle,
                                                                                            avg_speed_dir_components[0],
                                                                                            avg_speed_dir_components[0] * meters_per_second_to_mph,
                                                                                            avg_speed_dir_components[1]))

          #Unity components, just direction with speeds all 1.
          avg_dir_components = calcAvgSpeedAndDir(direction_tuples)
          scalar_speed_avg = scalar_speed_avg / speed_count
          wq_tests_data['nos8661070_wind_spd'] = scalar_speed_avg * meters_per_second_to_mph
          wq_tests_data['nos8661070_wind_dir_val'] = avg_dir_components[1]
          self.logger.debug("Platform: %s Avg Scalar Wind Speed: %f(m_s-1) %f(mph) Direction: %f" % (platform_handle,
                                                                                                     scalar_speed_avg,
                                                                                                     scalar_speed_avg * meters_per_second_to_mph,
                                                                                                     avg_dir_components[1]))
        else:
          self.logger.error("Platform: %s no wind speed or direction data available" % (platform_handle))

      self.logger.debug("Finished retrieving platform: %s datetime: %s" % (platform_handle, start_date.strftime('%Y-%m-%d %H:%M:%S')))

      return
  def get_nexrad_data(self, start_date, wq_tests_data):
    if self.logger:
      self.logger.debug("Start retrieving nexrad data datetime: %s" % (start_date.strftime('%Y-%m-%d %H:%M:%S')))

    #Collect the radar data for the boundaries.
    for boundary in self.site.contained_by:
      clean_var_bndry_name = boundary.name.lower().replace(' ', '_')

      platform_handle = 'nws.%s.radarcoverage' % (boundary.name)
      if self.logger:
        self.logger.debug("Start retrieving nexrad platfrom: %s" % (platform_handle))
      # Get the radar data for previous 8 days in 24 hour intervals
      for prev_hours in range(24, 192, 24):
        var_name = '%s_nexrad_summary_%d' % (clean_var_bndry_name, prev_hours)
        radar_val = self.xenia_wq_db.getLastNHoursSummaryFromRadarPrecip(platform_handle,
                                                                    start_date,
                                                                    prev_hours,
                                                                    'precipitation_radar_weighted_average',
                                                                    'mm')
        if radar_val != None:
          #Convert mm to inches
          wq_tests_data[var_name] = radar_val * 0.0393701
        else:
          if self.logger:
            self.logger.error("No data available for boundary: %s Date: %s. Error: %s" %(var_name, start_date, self.xenia_wq_db.getErrorInfo()))

      #calculate the X day delay totals
      if wq_tests_data['%s_nexrad_summary_48' % (clean_var_bndry_name)] != wq_defines.NO_DATA and\
         wq_tests_data['%s_nexrad_summary_24' % (clean_var_bndry_name)] != wq_defines.NO_DATA:
        wq_tests_data['%s_nexrad_total_1_day_delay' % (clean_var_bndry_name)] = wq_tests_data['%s_nexrad_summary_48' % (clean_var_bndry_name)] - wq_tests_data['%s_nexrad_summary_24' % (clean_var_bndry_name)]

      if wq_tests_data['%s_nexrad_summary_72' % (clean_var_bndry_name)] != wq_defines.NO_DATA and\
         wq_tests_data['%s_nexrad_summary_48' % (clean_var_bndry_name)] != wq_defines.NO_DATA:
        wq_tests_data['%s_nexrad_total_2_day_delay' % (clean_var_bndry_name)] = wq_tests_data['%s_nexrad_summary_72' % (clean_var_bndry_name)] - wq_tests_data['%s_nexrad_summary_48' % (clean_var_bndry_name)]

      if wq_tests_data['%s_nexrad_summary_96' % (clean_var_bndry_name)] != wq_defines.NO_DATA and\
         wq_tests_data['%s_nexrad_summary_72' % (clean_var_bndry_name)] != wq_defines.NO_DATA:
        wq_tests_data['%s_nexrad_total_3_day_delay' % (clean_var_bndry_name)] = wq_tests_data['%s_nexrad_summary_96' % (clean_var_bndry_name)] - wq_tests_data['%s_nexrad_summary_72' % (clean_var_bndry_name)]

      prev_dry_days = self.xenia_wq_db.getPrecedingRadarDryDaysCount(platform_handle,
                                             start_date,
                                             'precipitation_radar_weighted_average',
                                             'mm')
      if prev_dry_days is not None:
        var_name = '%s_nexrad_dry_days_count' % (clean_var_bndry_name)
        wq_tests_data[var_name] = prev_dry_days

      rainfall_intensity = self.xenia_wq_db.calcRadarRainfallIntensity(platform_handle,
                                                               start_date,
                                                               60,
                                                              'precipitation_radar_weighted_average',
                                                              'mm')
      if rainfall_intensity is not None:
        var_name = '%s_nexrad_rainfall_intensity' % (clean_var_bndry_name)
        wq_tests_data[var_name] = rainfall_intensity


      if self.logger:
        self.logger.debug("Finished retrieving nexrad platfrom: %s" % (platform_handle))

    if self.logger:
      self.logger.debug("Finished retrieving nexrad data datetime: %s" % (start_date.strftime('%Y-%m-%d %H:%M:%S')))
  def get_tide_data(self, start_date, wq_tests_data):
    if self.logger:
      self.logger.debug("Start retrieving tide data for station: %s date: %s" % (self.tide_station, start_date))

    use_web_service = True
    if self.tide_data_obj is not None:
      use_web_service = False
      date_key = start_date.strftime('%Y-%m-%dT%H:%M:%S')
      if date_key in self.tide_data_obj:
        tide_rec = self.tide_data_obj[date_key]
        wq_tests_data['tide_range_%s' % (self.tide_station)] = tide_rec['range']
        wq_tests_data['tide_hi_%s' % (self.tide_station)] = tide_rec['hi']
        wq_tests_data['tide_lo_%s' % (self.tide_station)] = tide_rec['lo']

        try:
          #Get previous 24 hours.
          tide_start_time = (start_date - timedelta(hours=24))
          tide_end_time = start_date

          tide = noaaTideData(use_raw=True, logger=self.logger)

          tide_stage = tide.get_tide_stage(begin_date = tide_start_time,
                             end_date = tide_end_time,
                             station=self.tide_station,
                             datum='MLLW',
                             units='feet',
                             time_zone='GMT')
          wq_tests_data['tide_stage_%s' % (self.tide_station)] = tide_stage

        except Exception as e:
          if self.logger:
            self.logger.exception(e)

      #THe web service is unreliable, so if we were using the history csv file, it may still be missing
      #a record, if so, let's try to look it up on the web.
      else:
        use_web_service = True
    if self.tide_data_obj is None or use_web_service:
      #Get previous 24 hours.
      tide_start_time = (start_date - timedelta(hours=24))
      tide_end_time = start_date

      tide = noaaTideData(use_raw=True, logger=self.logger)
      #Date/Time format for the NOAA is YYYYMMDD

      try:
        tide_data = tide.calcTideRange(beginDate = tide_start_time,
                           endDate = tide_end_time,
                           station=self.tide_station,
                           datum='MLLW',
                           units='feet',
                           timezone='GMT',
                           smoothData=False)

      except Exception as e:
        if self.logger:
          self.logger.exception(e)
      else:
        if tide_data and tide_data['HH'] is not None and tide_data['LL'] is not None:
          try:
            range = tide_data['HH']['value'] - tide_data['LL']['value']
          except TypeError as e:
            if self.logger:
              self.logger.exception(e)
          else:
            #Save tide station values.
            wq_tests_data['tide_range_%s' % (self.tide_station)] = range
            wq_tests_data['tide_hi_%s' % (self.tide_station)] = tide_data['HH']['value']
            wq_tests_data['tide_lo_%s' % (self.tide_station)] = tide_data['LL']['value']
            wq_tests_data['tide_stage_%s' % (self.tide_station)] = tide_data['tide_stage']
        else:
          if self.logger:
            self.logger.error("Tide data for station: %s date: %s not available or only partial." % (self.tide_station, start_date))

    if self.logger:
      self.logger.debug("Finished retrieving tide data for station: %s date: %s" % (self.tide_station, start_date))

    return
