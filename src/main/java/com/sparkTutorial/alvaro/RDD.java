package com.sparkTutorial.alvaro;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;
import java.util.List;

public class RDD {
    public static void main(String[] args) throws Exception {

        /** Configurar Spark */
        SparkConf conf = new SparkConf().setAppName("aprendizaje").setMaster("local[3]");

        /** Crear Contexto(SparkContext) donde incluiremos después el dataset
         * El SparkContext tiene muchos métodos que nos son últiles para trabajar con los datos**/
        JavaSparkContext sc = new JavaSparkContext(conf);


        /** Crear DataSet a partir de una lista **/
        //Crear un array con valores del 1 al 5 y meterlo en una Lista
        List<Integer> inputInteger = Arrays.asList(1, 2, 3, 4, 5);
        //Crear un dataset (JavaRDD) y meter la lista anterior en él con el parallelize.
        // Esto no es muy optimo en grandes datasets
        JavaRDD<Integer> integerRDD = sc.parallelize(inputInteger);


        /** Crear DataSet a partir de un fichero externo **/
        //Es mejor(más óptimo) crear el dataset(JavaRDD) a partir de un fichero externo.
        JavaRDD<String> lines = sc.textFile("in/uppercase.text");

        /** Como fuentes de entrada de datos, a parte de ficheros se pueden usar las siguientes:
         * Spark JDBC driver:
         * https://docs.databricks.com/spark/latest/data-sources/sql-databases.html
         *
         * Spark Cassandra connector:
         * http://www.datastax.com/dev/blog/kindling-an-introduction-to-spark-with-cassandra-part-1
         *
         * Spark Elasticsearch connector:
         * https://www.elastic.co/guide/en/elasticsearch/hadoop/current/spark.html
         * */

        /**  TRANSFORMACIONES
         *   Siempre partiremos de un dataset y generaremos uno nuevo transformado.
         */
        // TRANSFORMACIONES: FILTRO. Filtra las lineas del dataset que estén vacías
        JavaRDD<String> cleanedLines = lines.filter(line -> !lines.isEmpty());

        // TRANSFORMACIONES: MAPA. Transforma la slíneas de un fichero.
        JavaRDD<String> URLs = sc.textFile("in/urls.text");
        URLs.map(url -> makeHttpRequest(url));

    }

    private static String makeHttpRequest(String url) {
        return url;
    }
}
