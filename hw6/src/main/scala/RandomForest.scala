import java.util.Calendar

import com.datastax.spark.connector._
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.RandomForestClassifier
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created by Luciferre on 11/16/15.
 */
object RandomForest {
  def main(args: Array[String]) {

    val jarFile = "RandomForest.jar";
    val conf = new SparkConf().setAppName("RandomForest").setJars(Array(jarFile));
    conf.set("spark.cassandra.connection.host", "localhost")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    // Prepare training data and testing data from cassandra
    val train = sc.cassandraTable("bigdata", "train")
    val trainingData: RDD[LabeledPoint] = train.map { row => {
      (LabeledPoint(row.getInt("label").toDouble, Vectors.sparse(6, Array(0, 1, 2, 3, 4, 5), Array(row.getInt("bidMean"), row.getInt("askMean"), row.getInt("diff"), row.getInt("range"), row.getInt("spread")))))
    }
    }
    val training = sqlContext.createDataFrame(trainingData)


    val test = sc.cassandraTable("bigdata", "test")
    val testData: RDD[LabeledPoint] = test.map { row => {
      (LabeledPoint(row.getInt("label").toDouble, Vectors.sparse(6, Array(0, 1, 2, 3, 4, 5), Array(row.getInt("bidMean"), row.getInt("askMean"), row.getInt("diff"), row.getInt("range"), row.getInt("spread")))))
    }
    }
    val testing = sqlContext.createDataFrame(testData)


    val indexer = new StringIndexer().setInputCol("label").setOutputCol("indexedLabel")

    // Train a RandomForest model.
    val randomForest = new RandomForestClassifier().setNumTrees(5).setLabelCol("indexedLabel").setPredictionCol("predictedLabel")

    //Configure pipeline
    val pipeline = new Pipeline()
      .setStages(Array(indexer, randomForest))
    // Fit the pipeline to training documents.
    val model = pipeline.fit(training)


    // Make predictions on test documents.
    val predictions = model.transform(testing)

    // compute test error
    val evaluator = new MulticlassClassificationEvaluator()
    val accuracy = evaluator.evaluate(predictions)
    println("Accuracy = " + (1.0 - accuracy))

    //save performance to cassandra
    val collection = sc.parallelize(Seq((Calendar.getInstance().getTime(), accuracy)))
    collection.saveToCassandra("bigdata", "performance", SomeColumns("time", "accuracy"))

  }
}
