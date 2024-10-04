import org.apache.spark.ml.classification.NaiveBayes;
import org.apache.spark.ml.classification.NaiveBayesModel;
import org.apache.spark.ml.classification.RandomForestClassificationModel;
import org.apache.spark.ml.classification.RandomForestClassifier;
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator;
import org.apache.spark.ml.feature.StandardScaler;
import org.apache.spark.ml.feature.StringIndexer;
import org.apache.spark.ml.feature.VectorAssembler;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;

public class DiabetesApp {
    public static void main(String[] args) {

        SparkSession spark = SparkSession.builder()
                .master("local")
                .appName("diabetes-mllib")
                .getOrCreate();

        Dataset<Row> dataset = spark.read()
                .option("header", true)
                .option("inferSchema", true)
                .csv("C:\\Users\\Pantheon\\Desktop\\BigData\\Datasets\\MLlib\\diabetes.csv");
//        dataset.show();

        // --Data preprocessing--
        String[] headers = {"Pregnancies", "Glucose", "BloodPressure", "SkinThickness",
                "Insulin", "BMI", "DiabetesPedigreeFunction", "Age"};

        VectorAssembler vectorAssembler = new VectorAssembler()
                .setInputCols(headers)
                .setOutputCol("features");
        Dataset<Row> transformDF = vectorAssembler.transform(dataset);

        // Scaling
        StandardScaler scaler = new StandardScaler()
                .setInputCol("features")
                .setOutputCol("scaledFeatures")
                .setWithStd(true)
                .setWithMean(false);
        Dataset<Row> scaledDF = scaler.fit(transformDF)
                .transform(transformDF);

        Dataset<Row> finalDF = scaledDF.select("scaledFeatures", "Outcome")
                .withColumnRenamed("scaledFeatures", "features");

        // train-test split
        Dataset<Row>[] splits = finalDF.randomSplit(new double[]{0.7, 0.3},42);
        Dataset<Row> trainData = splits[0];
        Dataset<Row> testData = splits[1];

        // --Create model--
        RandomForestClassifier rfc = new RandomForestClassifier()
                .setLabelCol("Outcome")
                .setFeaturesCol("features");

        // --Train model--
        RandomForestClassificationModel model = rfc.fit(trainData);

        // --Predictions--
        Dataset<Row> predictions = model.transform(testData);
        predictions.show();

        // --Evaluate model--
        MulticlassClassificationEvaluator evaluator = new MulticlassClassificationEvaluator()
                .setLabelCol("Outcome")
                .setPredictionCol("prediction")
                .setMetricName("accuracy");
        double evaluate = evaluator.evaluate(predictions);
        System.out.println("accuracy : " + evaluate);
    }
}

