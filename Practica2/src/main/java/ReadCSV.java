import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.count;
public class ReadCSV {
    public static void main(String[] args) {
        SparkSession sparkSession=SparkSession
                .builder()
                .master("local")
                .appName("Read_CSV")
                .getOrCreate();

        Dataset<Row> csvData = sparkSession
                .sqlContext()
                .read()
                .option("header", "true")
                .format("csv")
                .load("C:\\Users\\ELY\\Downloads\\Erasmus.csv");

        Dataset<Row> result = csvData.filter(col("Sending Country Code").notEqual(col("Receiving Country Code")))
                .groupBy("Sending Country Code", "Receiving Country Code")
                .agg(count("Sending Country Code").alias("Student Count"))
                .orderBy("Sending Country Code", "Receiving Country Code");
        result.show();
        csvData.printSchema();
        csvData.show(false);
        sparkSession.stop();
    }
}
