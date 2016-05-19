import sys
from os.path import dirname, realpath
sys.path.append('../../commonfiles/python')
sys.path.append(dirname(realpath(__file__)))
from datetime import datetime
from pytz import timezone

from wq_prediction_plugin import wq_prediction_engine_plugin
from mb_wq_prediction_engine import run_wq_models

class mb_prediction_plugin(wq_prediction_engine_plugin):

  def inititalize_plugin(self, **kwargs):
    self.logger.debug("inititalize_plugin Started")
    self.config_file = kwargs['ini']
    self.process_dates = kwargs.get('process_date', None)
    self.logger.debug("inititalize_plugin Finished")

  def run_wq_models(self, **kwargs):
    self.logger.debug("run_wq_models Started")
    dates_to_process = []
    if self.process_dates is not None:
      #Can be multiple dates, so let's split on ','
      collection_date_list = self.process_dates.split(',')
      #We are going to process the previous day, so we get the current date, set the time to midnight, then convert
      #to UTC.
      eastern = timezone('US/Eastern')
      try:
        for collection_date in collection_date_list:
          est = eastern.localize(datetime.strptime(collection_date, "%Y-%m-%dT%H:%M:%S"))
          #Convert to UTC
          begin_date = est.astimezone(timezone('UTC'))
          dates_to_process.append(begin_date)
      except Exception,e:
        self.logger.exception(e)
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
        run_wq_models(begin_date=process_date,
                      config_file_name=self.config_file)
    except Exception, e:
      self.logger.exception(e)
    self.logger.debug("run_wq_models Finished")
