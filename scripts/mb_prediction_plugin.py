import sys
from os.path import dirname, realpath
sys.path.append('../commonfiles/python')
sys.path.append(dirname(realpath(__file__)))

from wq_prediction_plugin import wq_prediction_engine_plugin
from mb_wq_prediction_engine import mb_prediction_engine

class mb_prediction_plugin(wq_prediction_engine_plugin):

  def do_processing(self, **kwargs):
    dates_to_process = kwargs.get('processing_dates', [])
    for process_date in dates_to_process:
      mb_engine = mb_prediction_engine()
      mb_engine.run_wq_models(begin_date=process_date,
                      config_file_name=self.config_file)

