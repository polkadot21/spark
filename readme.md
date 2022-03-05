# Spark implementation of K-means clustering with the DTW distance

This repository contains the k-means algorithm implementation in pyspark API. The algorithm is adjusted to be used with 
time-series data by the application of the DTW distance.

## Usage

```bash
#allow access
chmod +x /.setup.sh

# setup
/.setup.sh

# run functions
python3 k_mean_clustering_spark.py

```