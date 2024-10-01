import org.apache.spark.ml.classification.NaiveBayes;
import org.apache.spark.ml.classification.NaiveBayesModel;
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator;
import org.apache.spark.ml.feature.StringIndexer;
import org.apache.spark.ml.feature.VectorAssembler;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;

public class MySample {
    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder()
                .master("local")
                .appName("diabetes-mllib")
                .getOrCreate();

        Dataset<Row> basketDS = spark.read().format("csv")
                .option("header", true)
                .option("inferSchema", true)
                .load("C:\\Users\\Pantheon\\Desktop\\BigData\\Datasets\\MLlib\\basketbol.csv");
        basketDS.show();

        // --Data preprocessing--
        String[] headerList = {"hava", "sicaklik", "nem", "ruzgar", "basketbol"};
        List<String> headers = Arrays.asList(headerList);

        List<String> headersResult = new ArrayList<String>();
        for (String h : headers){
            if (h.equals("basketbol")){
                StringIndexer tmpIndexer = new StringIndexer().setInputCol(h).setOutputCol("label");
                basketDS = tmpIndexer.fit(basketDS).transform(basketDS);
                headersResult.add("label");
            } else {
                StringIndexer tmpIndexer = new StringIndexer().setInputCol(h).setOutputCol(h.toLowerCase() + "_index");
                basketDS = tmpIndexer.fit(basketDS).transform(basketDS);
                headersResult.add(h.toLowerCase() + "_index");
            }
        }

        String[] colList = headersResult.toArray(new String[headersResult.size()]);
        VectorAssembler vectorAssembler = new VectorAssembler()
                .setInputCols(colList)
                .setOutputCol("features");
        Dataset<Row> transformedDS = vectorAssembler.transform(basketDS);
        Dataset<Row> finalDS = transformedDS.select("features", "label");
        finalDS.show();

        //train-test split
        Dataset<Row>[] splits = finalDS.randomSplit(new double[]{0.7, 0.3},42);
        Dataset<Row> trainData = splits[0];
        Dataset<Row> testData = splits[1];

        // --Create model--
        NaiveBayes nb = new NaiveBayes();
        nb.setSmoothing(1);

        // --Train model--
        NaiveBayesModel model = nb.fit(trainData);

        // --Predictions--
        Dataset<Row> predictions = model.transform(testData);
        predictions.show();

        // --Evaluate Model--
        MulticlassClassificationEvaluator evaluator = new MulticlassClassificationEvaluator()
                .setLabelCol("label")
                .setPredictionCol("prediction")
                .setMetricName("accuracy");
        double evaluate = evaluator.evaluate(predictions);
        System.out.println("accuracy : " + evaluate);
    }
}
