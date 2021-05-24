import sys
sys.path.append('../../commonfiles/python')
import os

import logging.config
from datetime import datetime
from pytz import timezone
import traceback
import time
import optparse
if sys.version_info[0] < 3:
  import ConfigParser
else:
  import configparser as ConfigParser
from collections import OrderedDict
#import simplejson as json
import json
from data_collector_plugin import data_collector_plugin

from yapsy.PluginManager import PluginManager
from multiprocessing import Queue

from wq_prediction_tests import wqEquations
from enterococcus_wq_test import EnterococcusPredictionTest,EnterococcusPredictionTestEx

from mb_wq_data import mb_wq_model_data, mb_sample_sites
from output_plugin import output_plugin
from wq_prediction_engine import wq_prediction_engine
from stats import stats

'''
Function: build_test_objects
Purpose: Builds the models used for doing the predictions.
Parameters:
  config_file - ConfigParser object
  site_name - The name of the site whose models we are building.
  use_logging - Flag to specify if we are to use logging.
Return:
  A list of models constructed.
'''
def build_test_objects(config_file, site_name):
  logger = logging.getLogger(__name__)

  model_list = []
  #Get the sites test configuration ini, then build the test objects.
  try:
    test_config_file = config_file.get(site_name, 'prediction_config')
    entero_lo_limit = config_file.getint('entero_limits', 'limit_lo')
    entero_hi_limit = config_file.getint('entero_limits', 'limit_hi')
  except ConfigParser.Error as e:
    if logger:
      logger.exception(e)
  else:
    if logger:
      logger.debug("Site: %s Model Config File: %s" % (site_name, test_config_file))

    model_config_file = ConfigParser.RawConfigParser()
    model_config_file.read(test_config_file)
    #Get the number of prediction models we use for the site.
    model_count = model_config_file.getint("settings", "model_count")
    if logger:
      logger.debug("Site: %s Model count: %d" % (site_name, model_count))

    for cnt in range(model_count):
      model_name = model_config_file.get("model_%d" % (cnt+1), "name")
      model_equation = model_config_file.get("model_%d" % (cnt+1), "formula")
      if logger:
        logger.debug("Site: %s Model name: %s equation: %s" % (site_name, model_name, model_equation))

      test_obj = EnterococcusPredictionTestEx(model_equation, site_name, model_name)
      test_obj.set_category_limits(entero_lo_limit, entero_hi_limit)
      model_list.append(test_obj)

  return model_list

'''
Function: check_site_date_for_sampling_date
Purpose: For the given site, we check the stations bacteria samples to see
 if there is a field sample that occured as well. If so, we return it.
Return:
  Entero value if found, otherwise None.
'''
def check_site_date_for_sampling_date(site_name, test_date, output_settings_ini, use_logging):
  entero_value = None
  logger = None
  if use_logging:
    logger = logging.getLogger('check_site_date_for_sampling_date_logger')
    logger.debug("Starting check_site_date_for_sampling_date. Site: %s Date: %s" % (site_name, test_date))
  try:
    config_file = ConfigParser.RawConfigParser()
    config_file.read(output_settings_ini)
    station_results_directory = config_file.get('output', 'station_results_directory')
  except ConfigParser.Error as e:
    if logger:
      logger.exception(e)
  else:
    station_bacteria_filename = os.path.join(station_results_directory, '%s.json' % (site_name))
    try:
      with open(station_bacteria_filename, 'r') as station_bacteria_json_file:
        station_json_data = json.loads(station_bacteria_json_file.read())
      if station_json_data is not None:
        properties = station_json_data['properties']
        test_results = properties['test']
        for result in test_results['beachadvisories']:
          result_date = timezone('UTC').localize(datetime.strptime(result['date'], '%Y-%m-%d %H:%M:%S'))
          if result_date.date() == test_date.date():
            if not isinstance(result['value'], list):
              entero_value = result['value']
            else:
              entero_value = result['value'][0]
            break
    except IOError as e:
      if logger:
        logger.exception(e)

  if logger:
    logger.debug("Finished check_site_date_for_sampling_date. Site: %s Date: %s" % (site_name, test_date))

  return entero_value

def run_wq_models(**kwargs):
  logger = logging.getLogger(__name__)
  prediction_testrun_date = datetime.now()


  try:
    config_file = ConfigParser.RawConfigParser()
    config_file.read(kwargs['config_file_name'])

    boundaries_location_file = config_file.get('boundaries_settings', 'boundaries_file')
    sites_location_file = config_file.get('boundaries_settings', 'sample_sites')
    xenia_wq_db_file = config_file.get('database', 'name')

    #MOve xenia obs db settings into standalone ini. We can then
    #check the main ini file into source control without exposing login info.
    db_settings_ini = config_file.get('password_protected_configs', 'settings_ini')
    xenia_obs_db_config_file = ConfigParser.RawConfigParser()
    xenia_obs_db_config_file.read(db_settings_ini)

    xenia_obs_db_host = xenia_obs_db_config_file.get('xenia_observation_database', 'host')
    xenia_obs_db_user = xenia_obs_db_config_file.get('xenia_observation_database', 'user')
    xenia_obs_db_password = xenia_obs_db_config_file.get('xenia_observation_database', 'password')
    xenia_obs_db_name = xenia_obs_db_config_file.get('xenia_observation_database', 'database')

    #output results config file. Again split out into individual ini file
    #for security.
    output_settings_ini = config_file.get('password_protected_configs', 'settings_ini')

    output_plugin_dirs=config_file.get('output_plugins', 'plugin_directories').split(',')
  except ConfigParser.Error as e:
    if logger:
      logger.exception(e)
  else:
    #Load the sample site information. Has name, location and the boundaries that contain the site.
    mb_sites = mb_sample_sites()
    mb_sites.load_sites(file_name=sites_location_file, boundary_file=boundaries_location_file)
    #Retrieve the data needed for the models.

    mb_wq_data = mb_wq_model_data(xenia_wq_db_name=xenia_wq_db_file,
                                  xenia_obs_db_type='postgresql',
                                  xenia_obs_db_host=xenia_obs_db_host,
                                  xenia_obs_db_user=xenia_obs_db_user,
                                  xenia_obs_db_password=xenia_obs_db_password,
                                  xenia_obs_db_name=xenia_obs_db_name
                                  )

    site_model_ensemble = []
    #First pass we want to get all the data, after that we only need to query
    #the site specific pieces.
    reset_site_specific_data_only = False
    site_data = OrderedDict()
    total_time = 0
    for site in mb_sites:
      try:
        #Get all the models used for the particular sample site.
        model_list = build_test_objects(config_file, site.name)
        #Create the container for all the models.
        site_equations = wqEquations(site.name, model_list, True)

        #Get the station specific tide stations
        tide_station = config_file.get(site.name, 'tide_station')
        #We use the virtual tide sites as there no stations near the sites.
      except ConfigParser.Error as e:
        if logger:
          logger.exception(e)
      else:
        mb_wq_data.reset(site=site,
                          tide_station=tide_station
                          )

        site_data['station_name'] = site.name
        try:
          mb_wq_data.query_data(kwargs['begin_date'], kwargs['begin_date'], site_data, reset_site_specific_data_only)
          reset_site_specific_data_only = True
          site_equations.runTests(site_data)
          total_test_time = sum(testObj.test_time for testObj in site_equations.tests)
          if logger:
            logger.debug("Site: %s total time to execute models: %f ms" % (site.name, total_test_time * 1000))
          total_time += total_test_time

          """
          #Calculate some statistics on the entero results. This is making an assumption
          #that all the tests we are running are calculating the same value, the entero
          #amount.
          entero_stats = None
          if len(site_equations.tests):
            entero_stats = stats()
            for test in site_equations.tests:
              if test.mlrResult is not None:
                entero_stats.addValue(test.mlrResult)
            entero_stats.doCalculations()
          #Check to see if there is a entero sample for our date as long as the date
          #is not the current date.
          entero_value = None
          if datetime.now().date() != kwargs['begin_date'].date():
            entero_value = check_site_date_for_sampling_date(site.name, kwargs['begin_date'], output_settings_ini, kwargs['use_logging'])
          """

          site_model_ensemble.append({'metadata': site,
                                      'models': site_equations,
                                      'entero_value': None})
        except Exception as e:
          if logger:
            logger.exception(e)

    if logger:
      logger.debug("Total time to execute all sites models: %f ms" % (total_time * 1000))

    run_output_plugins(output_plugin_directories=output_plugin_dirs,
                        site_model_ensemble=site_model_ensemble,
                         prediction_date=kwargs['begin_date'],
                         prediction_run_date=prediction_testrun_date)
    """
    output_results(site_model_ensemble=site_model_ensemble,
                   config_file_name=output_settings_ini,
                   prediction_date=kwargs['begin_date'],
                   prediction_run_date=prediction_testrun_date)
    """
  return

def run_output_plugins(**kwargs):
  logger = logging.getLogger(__name__)
  logger.info("Begin run_output_plugins")

  simplePluginManager = PluginManager()
  logging.getLogger('yapsy').setLevel(logging.DEBUG)
  simplePluginManager.setCategoriesFilter({
     "OutputResults": output_plugin
     })

  # Tell it the default place(s) where to find plugins
  if logger:
    logger.debug("Plugin directories: %s" % (kwargs['output_plugin_directories']))
  simplePluginManager.setPluginPlaces(kwargs['output_plugin_directories'])

  simplePluginManager.collectPlugins()

  plugin_cnt = 0
  plugin_start_time = time.time()
  for plugin in simplePluginManager.getAllPlugins():
    if logger:
      logger.info("Starting plugin: %s" % (plugin.name))
    if plugin.plugin_object.initialize_plugin(details=plugin.details):
      plugin.plugin_object.emit(prediction_date=kwargs['prediction_date'].astimezone(timezone("US/Eastern")).strftime("%Y-%m-%d %H:%M:%S"),
                                execution_date=kwargs['prediction_run_date'].strftime("%Y-%m-%d %H:%M:%S"),
                                ensemble_tests=kwargs['site_model_ensemble'])
      plugin_cnt += 1
    else:
      logger.error("Failed to initialize plugin: %s" % (plugin.name))
  logger.debug("%d output plugins run in %f seconds" % (plugin_cnt, time.time() - plugin_start_time))
  logger.info("Finished run_output_plugins")
"""
def output_results(**kwargs):
  logger = logging.getLogger(__name__)
  logger.debug("Starting output_results")
  record = {
    'prediction_date': kwargs['prediction_date'].astimezone(timezone("US/Eastern")).strftime("%Y-%m-%d %H:%M:%S"),
    'execution_date': kwargs['prediction_run_date'].strftime("%Y-%m-%d %H:%M:%S"),
    'ensemble_tests': kwargs['site_model_ensemble']
  }
  try:
    results_out = results_exporter(True)
    results_out.load_configuration(kwargs['config_file_name'])
    results_out.output(record)
  except Exception as e:
    if logger:
      logger.exception(e)

  if logger:
    logger.debug("Finished output_results")
  return
"""

class mb_prediction_engine(wq_prediction_engine):
  def __init__(self):
    self.logger = logging.getLogger(type(self).__name__)

  def build_test_objects(self, **kwargs):
    config_file = kwargs['config_file']
    site_name = kwargs['site_name']

    model_list = []
    #Get the sites test configuration ini, then build the test objects.
    try:
      test_config_file = config_file.get(site_name, 'prediction_config')
      entero_lo_limit = config_file.getint('entero_limits', 'limit_lo')
      entero_hi_limit = config_file.getint('entero_limits', 'limit_hi')
    except ConfigParser.Error as e:
        self.logger.exception(e)
    else:
      self.logger.debug("Site: %s Model Config File: %s" % (site_name, test_config_file))

      model_config_file = ConfigParser.RawConfigParser()
      model_config_file.read(test_config_file)
      #Get the number of prediction models we use for the site.
      model_count = model_config_file.getint("settings", "model_count")
      self.logger.debug("Site: %s Model count: %d" % (site_name, model_count))

      for cnt in range(model_count):
        model_name = model_config_file.get("model_%d" % (cnt+1), "name")
        model_equation = model_config_file.get("model_%d" % (cnt+1), "formula")
        self.logger.debug("Site: %s Model name: %s equation: %s" % (site_name, model_name, model_equation))

        test_obj = EnterococcusPredictionTestEx(formula=model_equation,
                                                site_name=site_name,
                                                model_name=model_name)
        test_obj.set_category_limits(entero_lo_limit, entero_hi_limit)
        model_list.append(test_obj)

    return model_list

  def run_wq_models(self, **kwargs):
    prediction_testrun_date = datetime.now()
    try:
      config_file = ConfigParser.RawConfigParser()
      config_file.read(kwargs['config_file_name'])

      enable_data_collector_plugins = config_file.getboolean('data_collector_plugins', 'enable_plugins')
      data_collector_plugin_directories = config_file.get('data_collector_plugins', 'plugin_directories').split(',')

      if enable_data_collector_plugins:
        self.collect_data(data_collector_plugin_directories=data_collector_plugin_directories)


      boundaries_location_file = config_file.get('boundaries_settings', 'boundaries_file')
      sites_location_file = config_file.get('boundaries_settings', 'sample_sites')
      xenia_wq_db_file = config_file.get('database', 'name')

      #MOve xenia obs db settings into standalone ini. We can then
      #check the main ini file into source control without exposing login info.
      db_settings_ini = config_file.get('password_protected_configs', 'settings_ini')
      xenia_obs_db_config_file = ConfigParser.RawConfigParser()
      xenia_obs_db_config_file.read(db_settings_ini)

      xenia_obs_db_host = xenia_obs_db_config_file.get('xenia_observation_database', 'host')
      xenia_obs_db_user = xenia_obs_db_config_file.get('xenia_observation_database', 'user')
      xenia_obs_db_password = xenia_obs_db_config_file.get('xenia_observation_database', 'password')
      xenia_obs_db_name = xenia_obs_db_config_file.get('xenia_observation_database', 'database')

      enable_output_plugins = config_file.getboolean('output_plugins', 'enable_plugins')
      output_plugin_dirs=config_file.get('output_plugins', 'plugin_directories').split(',')
    except (ConfigParser.Error, Exception) as e:
      self.logger.exception(e)
    else:
      #Load the sample site information. Has name, location and the boundaries that contain the site.
      mb_sites = mb_sample_sites()
      mb_sites.load_sites(file_name=sites_location_file, boundary_file=boundaries_location_file)
      #Retrieve the data needed for the models.

      mb_wq_data = mb_wq_model_data(xenia_wq_db_name=xenia_wq_db_file,
                                    xenia_obs_db_type='postgresql',
                                    xenia_obs_db_host=xenia_obs_db_host,
                                    xenia_obs_db_user=xenia_obs_db_user,
                                    xenia_obs_db_password=xenia_obs_db_password,
                                    xenia_obs_db_name=xenia_obs_db_name
                                    )

      site_model_ensemble = []
      #First pass we want to get all the data, after that we only need to query
      #the site specific pieces.
      reset_site_specific_data_only = False
      site_data = OrderedDict()
      total_time = 0
      for site in mb_sites:
        try:
          #Get all the models used for the particular sample site.
          model_list = self.build_test_objects(config_file=config_file, site_name=site.name)
          if len(model_list):
            #Create the container for all the models.
            site_equations = wqEquations(site.name, model_list, True)

            #Get the station specific tide stations
            tide_station = config_file.get(site.name, 'tide_station')
          else:
            self.logger.error("No models found for site: %s" % (site.name))
        except (ConfigParser.Error,Exception) as e:
          self.logger.exception(e)
        else:
          try:
            if len(model_list):
              mb_wq_data.reset(site=site,
                                tide_station=tide_station
                                )

              site_data['station_name'] = site.name
              mb_wq_data.query_data(kwargs['begin_date'], kwargs['begin_date'], site_data, reset_site_specific_data_only)
              reset_site_specific_data_only = True
              site_equations.runTests(site_data)
              total_test_time = sum(testObj.test_time for testObj in site_equations.tests)
              self.logger.debug("Site: %s total time to execute models: %f ms" % (site.name, total_test_time * 1000))
              total_time += total_test_time


              #Calculate some statistics on the entero results. This is making an assumption
              #that all the tests we are running are calculating the same value, the entero
              #amount.
              entero_stats = None
              if len(site_equations.tests):
                entero_stats = stats()
                for test in site_equations.tests:
                  if test.mlrResult is not None:
                    entero_stats.addValue(test.mlrResult)
                entero_stats.doCalculations()

              #Check to see if there is a entero sample for our date as long as the date
              #is not the current date.
              entero_value = None
              if datetime.now().date() != kwargs['begin_date'].date():
                entero_value = check_site_date_for_sampling_date(site.name, kwargs['begin_date'], output_settings_ini, kwargs['use_logging'])


              site_model_ensemble.append({'metadata': site,
                                          'models': site_equations,
                                          'entero_value': None,
                                          'statistics': entero_stats})
          except Exception as e:
            self.logger.exception(e)

      self.logger.debug("Total time to execute all sites models: %f ms" % (total_time * 1000))
      try:
        if enable_output_plugins:
          self.output_results(output_plugin_directories=output_plugin_dirs,
                                  site_model_ensemble=site_model_ensemble,
                                  prediction_date=kwargs['begin_date'],
                                  prediction_run_date=prediction_testrun_date)
      except Exception as e:
        self.logger.exception(e)
    return

  def collect_data(self, **kwargs):
    self.logger.info("Begin collect_data")
    try:
      simplePluginManager = PluginManager()
      yapsy_log = logging.getLogger('yapsy')
      yapsy_log.setLevel(logging.DEBUG)
      yapsy_log.disabled = False
      simplePluginManager.setCategoriesFilter({
         "DataCollector": data_collector_plugin
         })

      # Tell it the default place(s) where to find plugins
      self.logger.debug("Plugin directories: %s" % (kwargs['data_collector_plugin_directories']))
      simplePluginManager.setPluginPlaces(kwargs['data_collector_plugin_directories'])

      simplePluginManager.collectPlugins()

      output_queue = Queue()
      plugin_cnt = 0
      plugin_start_time = time.time()
      for plugin in simplePluginManager.getAllPlugins():
        self.logger.info("Starting plugin: %s" % (plugin.name))
        if plugin.plugin_object.initialize_plugin(details=plugin.details,
                                                  queue=output_queue):
          plugin.plugin_object.start()
        else:
          self.logger.error("Failed to initialize plugin: %s" % (plugin.name))
        plugin_cnt += 1

      #Wait for the plugings to finish up.
      self.logger.info("Waiting for %d plugins to complete." % (plugin_cnt))
      for plugin in simplePluginManager.getAllPlugins():
        plugin.plugin_object.join()
        plugin.plugin_object.finalize()
      while not output_queue.empty():
        results = output_queue.get()
        if results[0] == data_result_types.MODEL_DATA_TYPE:
          self.site_data = results[1]

      self.logger.info("%d Plugins completed in %f seconds" % (plugin_cnt, time.time() - plugin_start_time))
    except Exception as e:
      self.logger.exception(e)

def main():
  parser = optparse.OptionParser()
  parser.add_option("-c", "--ConfigFile", dest="config_file",
                    help="INI Configuration file." )
  parser.add_option("-s", "--StartDateTime", dest="start_date_time",
                    help="A date to re-run the predictions for, if not provided, the default is the current day. Format is YYYY-MM-DD HH:MM:SS." )

  (options, args) = parser.parse_args()

  if(options.config_file is None):
    parser.print_help()
    sys.exit(-1)

  try:
    config_file = ConfigParser.RawConfigParser()
    config_file.read(options.config_file)

    logger = None
    use_logging = False
    logConfFile = config_file.get('logging', 'prediction_engine')
    if logConfFile:
      logging.config.fileConfig(logConfFile)
      logger = logging.getLogger(__name__)
      logger.info("Log file opened.")
      use_logging = True

  except ConfigParser.Error as e:
    traceback.print_exc(e)
    sys.exit(-1)
  else:
    dates_to_process = []
    if options.start_date_time is not None:
      #Can be multiple dates, so let's split on ','
      collection_date_list = options.start_date_time.split(',')
      #We are going to process the previous day, so we get the current date, set the time to midnight, then convert
      #to UTC.
      eastern = timezone('US/Eastern')
      try:
        for collection_date in collection_date_list:
          est = eastern.localize(datetime.strptime(collection_date, "%Y-%m-%dT%H:%M:%S"))
          #Convert to UTC
          begin_date = est.astimezone(timezone('UTC'))
          dates_to_process.append(begin_date)
      except Exception as e:
        if logger:
          logger.exception(e)
    else:
      #We are going to process the previous day, so we get the current date, set the time to midnight, then convert
      #to UTC.
      est = datetime.now(timezone('US/Eastern'))
      est = est.replace(hour=0, minute=0, second=0,microsecond=0)
      #Convert to UTC
      begin_date = est.astimezone(timezone('UTC'))
      dates_to_process.append(begin_date)

    try:
      for process_date in dates_to_process:
        mb_engine = mb_prediction_engine()
        mb_engine.run_wq_models(begin_date=process_date,
                        config_file_name=options.config_file)
        #run_wq_models(begin_date=process_date,
        #              config_file_name=options.config_file)
    except Exception as e:
      logger.exception(e)

  if logger:
    logger.info("Log file closed.")

  return

if __name__ == "__main__":
  main()
