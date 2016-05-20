<?php
$SPARK_HOME = "/home/gt/spark/";
require($SPARK_HOME . "/php/src/context.php");
require($SPARK_HOME . "/php/src/sql/sql_context.php");

$sc = new context();
$sqlContext = new sql_context($sc);

$some_rdd = $sc->text_file("/home/gt/sql2.txt");

$schema = new StructType(
    array(
        new StructField("person_name", new StringType(), False),
        new StructField("person_age", new IntegerType(), False))
);

$some_df = $sqlContext->createDataFrame($some_rdd,$schema);
$some_df->printSchema();


/*
foreach ($some_df->filter("person_age > 20")->collect() as $e){
    print($e);
    print("!!!!!!!!!!!!!!!!!!!!!!!!");
};
*/

$people = $sqlContext->jsonFile("/home/gt/spark/examples/src/main/resources/people.json");

$people->registerAsTable("people2");
$some_df->registerAsTable("people");

$teenagers = $sqlContext->sql("SELECT * FROM people2 where age > 20");
print("---------------------------\n");
foreach($teenagers->collect() as $e) {
    print_r($e);
    print("!!!!!!!!!!!!!!!!!!!!!\n");
}
print("---------------------------\n");

$sc->stop();