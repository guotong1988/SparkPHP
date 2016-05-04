<?php
$SPARK_HOME = "/home/gt/spark/";
require($SPARK_HOME . "/php/src/context.php");
#usage
#./bin/spark-submit --jars /home/gt/spark/examples/target/scala-2.10/spark-examples-1.6.0-hadoop2.6.0.jar ./bin/avroTest.php

$sc = new context();


$schema_rdd = $sc->text_file("/home/gt/test.avsc", 1)->collect();//avsc文件负责筛选字段
$schema = array();
foreach($schema_rdd as $ele){
    array_push($schema,$ele);
}

$conf0 = array();
$conf0["avro.schema.input.key"] = array_reduce($schema,
    function($x,$y){
        return $x.$y;
    });


$avro_rdd = $sc->newAPIHadoopFile(
    "/home/gt/test.avro",
    "org.apache.avro.mapreduce.AvroKeyInputFormat",
    "org.apache.avro.mapred.AvroKey",
    "org.apache.hadoop.io.NullWritable",
    "org.apache.spark.examples.phpconverters.AvroWrapperToJavaConverter",
    "",$conf0,0);//$conf0如果填null则是全部字段


$output = $avro_rdd->map(
    function($x){
        return $x[0];
    }
)->collect();


foreach($output as $v0) {
    foreach($v0 as $v1){
        foreach($v1 as $k2=>$v2){
            print($k2);
            print("=>");
            print($v2);
            print("##");
        }
    }
    print("\n");
}


$sc->stop();

