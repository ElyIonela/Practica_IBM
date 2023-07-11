import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
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

        csvData.printSchema();
        csvData.show(false);
        sparkSession.stop();
    }
}
