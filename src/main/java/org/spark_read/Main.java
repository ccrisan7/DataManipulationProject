package org.spark_read;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;


public class Main {
    public static void main(String[] args) {

        //partea 1

        SparkSession spark = SparkSession
                .builder()
                .appName("Proiect_IBM")
                .master("local[*]")
                .getOrCreate();

        StructType schema = new StructType()
                .add("Project Reference", DataTypes.StringType)
                .add("Mobility Duration", DataTypes.StringType)
                .add("Participant Age", DataTypes.StringType)
                .add("Sending Country Code", DataTypes.StringType)
                .add("Receiving Country Code", DataTypes.StringType);

        Dataset<Row> df = spark
                .read()
                .option("delimiter", ",")
                .option("header", "true")
                .schema(schema)
                .csv("C://Users//VIVOBOOK//IdeaProjects//Proiect_BigData//src//Erasmus.csv");

        df = df
                .withColumnRenamed("Project Reference", "Cod proiect")
                .withColumnRenamed("Mobility Duration", "Durata mobilității)")
                .withColumnRenamed("Participant Age", "Vârsta participantului")
                .withColumnRenamed("Sending Country Code", "Codul țării de proveniență")
                .withColumnRenamed("Receiving Country Code", "Codul țării gazdă");

        df.show(30, false);
        df.printSchema();

        //partea 2

        List<String> listaTari1 = new ArrayList<>();

        listaTari1.add("FR");
        listaTari1.add("DE");
        listaTari1.add("AT");

        df = df
                .filter(df.col("Codul țării gazdă").isin(listaTari1.toArray()));

        df = df
                .groupBy("Codul țării gazdă", "Codul țării de proveniență")
                .count()
                .withColumnRenamed("count", "Număr de studenți")
                .orderBy("Codul țării gazdă", "Codul țării de proveniență");

        df.show(30);

        //partea 3

        Properties prop = new Properties();
        prop.setProperty("user", "root");
        prop.setProperty("password", "1234");
        String url = "jdbc:mysql://localhost:3306/ibm";

        df
                .write()
                .mode(SaveMode.Overwrite)
                .jdbc(url, "Statistica", prop);

        df = spark
                .read()
                .jdbc(url, "Statistica", prop);

        df.show(30);

        List<String> listaTari2 = new ArrayList<>();

        listaTari2.add("FR");
        listaTari2.add("DE");
        listaTari2.add("AT");


        for(String codTara : listaTari2) {
            Dataset<Row> tariDf = df
                    .filter(df.col("Codul țării gazdă").equalTo(codTara))
                    .drop("Codul țării gazdă");
            tariDf
                    .write()
                    .mode(SaveMode.Overwrite)
                    .jdbc(url, codTara, prop);
        }
    }
}