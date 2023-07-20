package org.spark_read;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;

public class Main {
    public static void main(String[] csv){
        SparkSession sparkSession = SparkSession.builder().master("local").appName("Read_CSV").getOrCreate();
        Dataset<Row> dataset = sparkSession.read().option("header","true").csv("C:\\Users\\VIVOBOOK\\IdeaProjects\\Proiect_BigData\\src\\Erasmus.csv");

        //dataset.printSchema();
        dataset.select("Receiving Country Code","Sending Country Code").show(20,false);
        dataset = dataset.filter(functions.col("Receiving Country Code").isin("LV", "MK", "MT"));
        dataset.groupBy("Receiving Country Code", "Sending Country Code")
                .count()
                .orderBy("Receiving Country Code", "Sending Country Code")
                .show(50);
    }
}
