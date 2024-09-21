# kafka_metrics
Repo with kafka metrics

To run the project, you have to:
  -Run kafka cluster with docker compose(You can use the docker compose example which attached to the repo).
  -Create a topic in the cluster.
  -Change the BOOTSTRAP_SERVERS in the consts file.
  -Run kafka_metrics.py with python 3.8.

dependencies: 
  -flusk
  -kafka-python
  -DateTime
  


