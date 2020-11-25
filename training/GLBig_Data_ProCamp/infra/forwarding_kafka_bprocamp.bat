gcloud compute ssh procamp-cluster-m ^
  --project=bigdata-procamp-9d63ef39 ^
  --zone=us-east1-b -- -D 1080 -N -L 9092:localhost:9092
  
  
