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

        Dataset<Row> df1 = spark
                .read()
                .option("delimiter", ",")
                .option("header", "true")
                .schema(schema)
                .csv("C://Users//VIVOBOOK//IdeaProjects//Proiect_BigData//src//Erasmus.csv")
                .withColumnRenamed("Project Reference", "Cod proiect")
                .withColumnRenamed("Mobility Duration", "Durata mobilității")
                .withColumnRenamed("Participant Age", "Vârsta participantului")
                .withColumnRenamed("Sending Country Code", "Codul țării de proveniență")
                .withColumnRenamed("Receiving Country Code", "Codul țării gazdă");

        df1.show(25, false);
        df1.printSchema();

        //partea 2

        List<String> listaTari1 = new ArrayList<>();

        listaTari1.add("FR");
        listaTari1.add("DE");
        listaTari1.add("AT");

        df1 = df1
                .filter(df1.col("Codul țării gazdă").isin(listaTari1.toArray()));

        df1 = df1
                .groupBy("Codul țării gazdă", "Codul țării de proveniență")
                .count()
                .withColumnRenamed("count", "Număr de studenți")
                .orderBy("Codul țării gazdă", "Codul țării de proveniență");

        df1.show(25);

        //partea 3

        String user = "root";
        String password = "1234";

        Properties prop = new Properties();

        prop.setProperty("user", user);
        prop.setProperty("password", password);
        String url = "jdbc:mysql://localhost:3306/ibm";

        df1
                .write()
                .mode(SaveMode.Overwrite)
                .jdbc(url, "Statistica", prop);

        df1 = spark
                .read()
                .jdbc(url, "Statistica", prop);

        df1.show(25);

        List<String> listaTariGazda1 = new ArrayList<>();

        listaTariGazda1.add("FR");
        listaTariGazda1.add("DE");
        listaTariGazda1.add("AT");


        for(String codTara : listaTariGazda1) {
            Dataset<Row> tariDf = df1
                    .filter(df1.col("Codul țării gazdă").equalTo(codTara))
                    .drop("Codul țării gazdă");
            tariDf
                    .write()
                    .mode(SaveMode.Overwrite)
                    .jdbc(url, codTara, prop);
        }

    //partea 4

    Dataset<Row> df2 = spark
            .read()
            .option("delimiter", ",")
            .option("header", "true")
            .schema(schema)
            .csv("C://Users//VIVOBOOK//IdeaProjects//Proiect_BigData//src//Erasmus.csv")
            .withColumnRenamed("Project Reference", "Cod proiect")
            .withColumnRenamed("Mobility Duration", "Durata mobilității")
            .withColumnRenamed("Participant Age", "Vârsta participantului")
            .withColumnRenamed("Sending Country Code", "Codul țării de proveniență")
            .withColumnRenamed("Receiving Country Code", "Codul țării gazdă");

    df2 = df2
            .groupBy("Codul țării gazdă", "Durata mobilității")
            .count()
                .withColumnRenamed("count", "Număr de studenți")
                .orderBy("Durata mobilității", "Codul țării gazdă");

    df2.show(25);

    df2
            .write()
            .mode(SaveMode.Overwrite).jdbc(url, "Statistica2", prop);

    df2 = spark
            .read()
            .jdbc(url, "Statistica2", prop);

    df2.show(25);

    List<String> listaTariGazda2 = new ArrayList<>();

    listaTariGazda2.add("RO");
    listaTariGazda2.add("HU");
    listaTariGazda2.add("BL");


    for(String codTara : listaTariGazda2) {
        Dataset<Row> tariDf = df2
                .filter(df2.col("Codul țării gazdă").equalTo(codTara))
                .drop("Codul țării gazdă");
        tariDf
                .write()
                .mode(SaveMode.Overwrite)
                .jdbc(url, codTara, prop);
        }
    }
}