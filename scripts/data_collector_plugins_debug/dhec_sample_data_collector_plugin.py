import sys
sys.path.append('../../commonfiles/python')
import logging.config
import data_collector_plugin as my_plugin

if sys.version_info[0] < 3:
  import ConfigParser
else:
  import configparser as ConfigParser
import traceback
import geojson

from dhecBeachAdvisoryReader import waterQualityAdvisory
from mb_wq_data import mb_sample_sites

class dhec_sample_data_collector_plugin(my_plugin.data_collector_plugin):

  def initialize_plugin(self, **kwargs):
    my_plugin.data_collector_plugin.initialize_plugin(self, **kwargs)
    try:
      logger = logging.getLogger(self.__class__.__name__)
      plugin_details = kwargs['details']
      self.ini_file = plugin_details.get('Settings', 'ini_file')
      return True
    except Exception as e:
      logger.exception(e)
    return False

  def run(self):
    try:
      configFile = ConfigParser.RawConfigParser()
      configFile.read(self.ini_file)
      log_file = configFile.get('logging', 'scraperConfigFile')
      logging.config.fileConfig(log_file)
      logger = logging.getLogger()
      logger.debug("run started.")

      """
      logger = None
      logConfFile = configFile.get('logging', 'scraperConfigFile')
      if(logConfFile):
        logging.config.fileConfig(logConfFile)
        logger = logging.getLogger("dhec_beach_advisory_app")
        logger.info("Log file opened.")
      """
    except ConfigParser.Error as e:
      print("No log configuration file given, logging disabled.")
    except Exception as e:
      import traceback
      traceback.print_exc()
      sys.exit(-1)
    try:
      logger.debug("Getting config params.")
      #Base URL to the page that house an individual stations results.
      baseUrl = configFile.get('websettings', 'baseAdvisoryPageUrl')

      #output Filename for the JSON data.
      jsonFilepath = configFile.get('output', 'outputDirectory')

      #Filepath to the geoJSON file that contains the station data for all the stations.
      stationGeoJsonFile = configFile.get('stationData', 'stationGeoJsonFile')

      #The past WQ results.
      stationWQHistoryFile = configFile.get('stationData', 'stationWQHistoryFile')

      dhec_rest_url = configFile.get('websettings', 'dhec_rest_url')

      boundaries_location_file = configFile.get('boundaries_settings', 'boundaries_file')
      sites_location_file = configFile.get('boundaries_settings', 'sample_sites')

      logger.debug("Finished getting config params.")
    except ConfigParser.Error as e:
      if(logger):
        logger.exception(e)

    else:
      try:
        mb_sites = mb_sample_sites()
        mb_sites.load_sites(file_name=sites_location_file, boundary_file=boundaries_location_file)

        logger.debug("Creating dhec sample query object.")
        advisoryObj = waterQualityAdvisory(baseUrl, True)
        #See if we have a historical WQ file, if so let's use that as well.
        logger.debug("Opening historical json file: %s." % (stationWQHistoryFile))
        historyWQFile = open(stationWQHistoryFile, "r")
        logger.debug("Loading historical json file: %s." % (stationWQHistoryFile))
        historyWQAll = geojson.load(historyWQFile)
        #Now cleanup and only have historical data from sites we do predictions on.
        historyWQ = {}
        for site in mb_sites:
          historyWQ[site.name] = historyWQAll[site.name]

        logger.debug("Beginning SOAP query.")
        advisoryObj.processData(
                                geo_json_file = stationGeoJsonFile,
                                json_file_path = jsonFilepath,
                                historical_wq = historyWQ,
                                dhec_url = dhec_rest_url,
                                post_data_url = None,
                                sampling_stations=mb_sites)
        logger.debug("Finished SOAP query.")
      except (IOError,Exception) as e:
        logger.exception(e)

    return
