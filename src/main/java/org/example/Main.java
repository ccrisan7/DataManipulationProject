package org.example;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;


class ReadCSV {
    public static void main(String[] args) {
        SparkSession sparkSession=SparkSession
                .builder()
                .master("local")
                .appName("Read_CSV")
                .getOrCreate();

        Dataset<Row> csvData = sparkSession
                .sqlContext()
                .read()
                .format("csv")
                .load("C:\\Users\\Acer\\Downloads\\Erasmus.csv");

        csvData.printSchema();
        csvData.show();
        sparkSession.stop();
    }
}