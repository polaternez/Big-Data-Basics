import org.apache.spark.ml.feature.*;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

public class MySample {
    public static void main(String[] args) {
        System.setProperty("hadoop.home.dir", "C:\\bigdata\\hadoop");

        SparkSession spark = SparkSession.builder()
                .master("local")
                .appName("spark-mllib")
                .getOrCreate();

        StructType schema = new StructType()
                .add("ulke", DataTypes.StringType)
                .add("boy", DataTypes.DoubleType)
                .add("kilo", DataTypes.DoubleType)
                .add("yas", DataTypes.DoubleType)
                .add("cinsiyet", DataTypes.StringType);
        Dataset<Row> dataset = spark.read()
                .option("header", true)
                .schema(schema)
                .csv("C:\\Users\\Pantheon\\Desktop\\BigData\\Datasets\\MLlib\\eksikveriler.csv");
        /*dataset.show();
        dataset.printSchema();*/

        /*Dataset<Row> newDF = dataset.withColumn("yas_int", dataset.col("yas").cast("int"));
        newDF.printSchema();*/

        // --Handle missing values--
        Imputer imputer = new Imputer()
                .setInputCols(new String[]{"yas"})
                .setOutputCols(new String[]{"yas_imputed"})
                .setStrategy("mean");
        dataset = imputer.fit(dataset).transform(dataset);

        // --Encode categorical variables--
        String[] catCols = {"ulke", "cinsiyet"};
        for (String col : catCols){
            StringIndexer indexer = new StringIndexer()
                    .setInputCol(col)
                    .setOutputCol(col + "_index");
            dataset = indexer.fit(dataset).transform(dataset);
        }

        OneHotEncoder oneHotEncoder = new OneHotEncoder()
                .setInputCols(new String[]{"ulke_index"})
                .setOutputCols(new String[]{"ulke_ohe"});
        dataset = oneHotEncoder.fit(dataset).transform(dataset);
        dataset.show();
    }
}
