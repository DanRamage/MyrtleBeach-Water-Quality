<!DOCTYPE html>

<html lang="en">
    <head>
      <meta name="viewport" content="width=device-width, initial-scale=1">
      <link href="http://howsthebeach.org/static/css/bootstrap/css/bootstrap.min.css" rel="stylesheet">
      <link href=http://howsthebeach.org//static/css/bootstrap/css/css/bootstrap-theme.min.css" rel="stylesheet">

      <title>Myrtle Beach Water Quality Prediction Results</title>
    </head>
    <body>
        <div class="container">
            <div class="row">
              <div class="col-xs-12">
                <h1>Myrtle Beach Quality Prediction Results</h1>
                <h2>Prediction for: ${prediction_date}</h2>
                <h3>Prediction executed: ${execution_date}</h3>
              </div>
            </div>
            </br>
            <div class = "row">
              <div class="col-xs-12">
                <h4><a href='${report_url}'>Report File</a> Click here if the report below displays incorrectly.</h4>
              </div>
            </div>
            % for site_data in ensemble_tests:
                <div class="row">
                    <div class="col-xs-6">
                      <h2>Site: ${site_data['metadata'].name}</h2>
                    </div>
                </div>
                <div class="row">
                    <div class="col-xs-2">
                    </div>
                    <div class="col-xs-6">
                      <h3><span>Overall Prediction: ${str(site_data['models'].ensemblePrediction)}</span></h3>
                    </div>
                </div>
            % endfor
        </div>
    </body>
</html>
