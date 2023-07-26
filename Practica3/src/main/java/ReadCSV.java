import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.count;
import org.apache.spark.sql.*;
import java.sql.SQLException;

public class ReadCSV {
    public static void main(String[] args) throws SQLException{
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
        dataBase(csvData, "ES", "Estonia");
        dataBase(csvData, "FR", "Franta");
        dataBase(csvData, "IT", "Italia");
        dataBase(csvData, "EL", "Elvetia");
        dataBase(csvData, "RO", "Romania");
        dataBase(csvData, "BG", "Bulgaria");
        dataBase(csvData, "CZ", "Cehia");
        //sparkSession.stop();
    }
    public static void dataBase(Dataset<Row> dataset, String countryCode, String tableName) {
        dataset
                .filter(col("Receiving Country Code").isin(countryCode))
                .groupBy("Receiving Country Code", "Sending Country Code")
                .count().orderBy("Receiving Country Code", "Sending Country Code")
                .write()
                .mode(SaveMode.Overwrite)
                .format("jdbc")
                .option("driver", "com.mysql.cj.jdbc.Driver")
                .option("url", "jdbc:mysql://localhost:3306/erasmus?serverTimezone=UTC")
                .option("dbtable", tableName)
                .option("user", "root")
                .option("password", "root")
                .save(tableName + ".erasmus");
    }
}
