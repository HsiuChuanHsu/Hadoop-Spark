# Hadoop & Spark Machine Learning 

## Introduction
In this project, we leverage the power of Hadoop, Spark MLlib, and Kafka to process and analyze the web log dataset, a large-scale dataset containing web server logs. By combining the strengths of these tools, we are able to effectively store, process, and analyze the large-scale ec_web_log dataset.


## System Architecture
![](./images/HadoopSpark.png)
Overview:
    - Data Storage and Processing with Hadoop: 
        Hadoop is leveraged to store and process large-scale datasets
    - Machine Learning with Spark MLlib: 
        Spark MLlib is utilized to perform feature engineering and model training on the processed data, enabling advanced analytics and predictive capabilities.
    - Real-Time Data Ingestion and Streaming with Kafka: 
        Kafka acts as the ingestion layer, collecting and buffering real-time data, which is then consumed by Spark Streaming for real-time data processing and prediction.

## What I Learned
- Setting up a big data storage with HDFS
- Machine Learning with Apache Spark
- Realtime data prodiction with Kafka and Spark streaming
- Containerizing your entire data engineering setup with Docker

## Getting Started
1. Clone the repository:
```bash

```

2. Navigate to the project directory:

3. Run Docker Compose to spin up the services:




## Instructions


### Hadoop
http://localhost:9870

![](./images/Hadoop_Namenode.png)
![](./images/Hadoop_data.png)


### Spark MLlib
http://127.0.0.1:8888
![](./images/Hadoop_Models.png)



### Kafka
http://localhost:9021
![](./images/Kafka_Streaming_Data.png)
![](./images/Prediction_data.png)
![](./images/Hadoop_PredictionResult.png)


