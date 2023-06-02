## Model deployment
[Tutorial](https://cloud.google.com/bigquery/docs/export-model-tutorial)

## Steps
gcloud auth login
### export our model from bigquery to google cloud storage
bq --project_id stoked-utility-387710 extract -m nytaxi.tip_model gs://taxi_ml_models/tip_model
### create a temp directory
mkdir /tmp/model
### copy our model to our temp directory
gsutil cp -r gs://taxi_ml_model/tip_model /tmp/model
### create a serving directory
mkdir -p serving_dir/tip_model/1
#### copy the model data into my directory
cp -r /tmp/model/tip_model/* serving_dir/tip_model/1
### pulling the tensorflow serving docker image
docker pull emacski/tensorflow-serving:latest
### run our docker image
docker run -p 8501:8501 --mount type=bind,source=`pwd`/serving_dir/tip_model,target=/models/tip_model -e MODEL_NAME=tip_model -t emacski/tensorflow-serving:latest &
# launch it on postman
curl -d '{"instances": [{"passenger_count":1, "trip_distance":12.2, "PULocationID":"193", "DOLocationID":"264", "payment_type":"2","fare_amount":20.4,"tolls_amount":0.0}]}' -X POST http://localhost:8501/v1/models/tip_model:predict
http://localhost:8501/v1/models/tip_model