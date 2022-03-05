from pyspark.sql import SparkSession
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.feature import StandardScaler
from pyspark.ml.clustering import KMeans
from pyspark.ml.evaluation import ClusteringEvaluator
import matplotlib.pyplot as plt
from dtw_udf import get_dtw_distance


def create_dataFrame(name, path) -> 'dataFrame':

    # define spark session
    spark_dataFrame = (SparkSession
             .builder
             .appName(name)
             .getOrCreate())\
        .read\
        .format("CSV")\
        .option("header", "true")\
        .option("inferSchema", "true")\
        .load(path)\
        .na.drop()  # drop NaN values
    return spark_dataFrame

def scale_data(dataset):
    scale=StandardScaler(inputCol='features',outputCol='standardized')
    scaled_dataset=scale.fit(dataset)
    data_scale_output=scaled_dataset.transform(dataset)
    return data_scale_output
#data_scale_output.show(2)

def k_means(dataset):
    silhouette_score = []
    evaluator = ClusteringEvaluator(predictionCol='prediction', featuresCol='standardized',
                                metricName='silhouette', distanceMeasure='squaredEuclidean')
    for i in range(2, 10):
        KMeans_algo = KMeans(featuresCol='standardized', k=i)

        KMeans_fit = KMeans_algo.fit(dataset)

        output = KMeans_fit.transform(dataset)

        score = evaluator.evaluate(output)

        silhouette_score.append(score)

        print("Silhouette Score_%s:" % i, score)
    return silhouette_score

def plot_metric_for_different_k(silhouette_score):
    # Visualizing the silhouette scores in a plot
    fig, ax = plt.subplots(1, 1, figsize=(8, 6))
    ax.plot(range(2, 10), silhouette_score)
    ax.set_xlabel("k")
    ax.set_ylabel("cost")
    return plt.show()

if __name__ == '__main__':
    path = "data/credit_cards.csv"  # credit card dataset
    name = 'Credit card clustering'

    data_customer = create_dataFrame(name, path)
    # vectorize data
    columns = data_customer.columns[1:]
    assemble = VectorAssembler(inputCols = columns,
                               outputCol='features')

    assembled_data = assemble.transform(data_customer)

    scaled_assembled_data = scale_data(assembled_data)
    sil_sc = k_means(scaled_assembled_data)
    plot_metric_for_different_k(sil_sc)
    #print(data_customer.columns[1:])