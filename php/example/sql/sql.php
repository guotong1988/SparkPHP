<?php
$SPARK_HOME = "/home/gt/spark/";
require($SPARK_HOME . "/php/src/context.php");
require($SPARK_HOME . "/php/src/sql/sql_context.php");

$sc = new context();
$sqlContext = new sql_context($sc);

$some_rdd = $sc->text_file("/home/gt/sql.txt");

$schema = new StructType(array(new StructField("person_name", new StringType(), False),
                               new StructField("person_age", new IntegerType(), False)));

$some_df = $sqlContext->createDataFrame($some_rdd,$schema);
$some_df->printSchema();