gcloud compute ssh procamp-cluster-m ^
  --project=bigdata-procamp-1add8fad ^
  --zone=us-east1-b -- -D 1080 -N -L 9092:localhost:9092
