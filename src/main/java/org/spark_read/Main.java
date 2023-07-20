package org.spark_read;

import org.apache.spark.sql.*;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import static org.apache.spark.sql.functions.col;

public class Main {
    public static void main(String[] csv) {
        SparkSession sparkSession = SparkSession.builder().master("local").appName("Read_CSV").getOrCreate();
        Dataset<Row> dataset = sparkSession.read().option("header","true").csv("C:\\Users\\VIVOBOOK\\IdeaProjects\\Proiect_BigData\\src\\Erasmus.csv");

        //dataset.printSchema();
        //dataset.select("Receiving Country Code","Sending Country Code").show(20,false);
        dataset = dataset.filter(functions.col("Receiving Country Code").isin("LV", "MK", "MT"));
        dataset.groupBy("Receiving Country Code", "Sending Country Code")
                .count()
                .orderBy("Receiving Country Code", "Sending Country Code")
                .show(50);

        saveData(dataset,"LV","letonia");
        saveData(dataset,"MK","macedonia_de_nord");
        saveData(dataset,"MT","malta");
    }

    public static void saveData(Dataset<Row> dataset, String countryCode, String tableName) {
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
                .option("password", "1234")
                .save(tableName + ".erasmus");
    }
}