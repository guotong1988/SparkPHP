<?php
$SPARK_HOME = "/home/gt/spark/";
require($SPARK_HOME . "/php/src/context.php");
require($SPARK_HOME . "/php/src/streaming/streaming_context.php");
#include_once($SPARK_HOME."/php/src/report_error.php");
#usage:  ./spark-submit --jars /home/gt/spark/external/kafka-assembly/target/spark-streaming-kafka-assembly_2.10-1.6.0.jar kafka_wordcount.php

$sc = new context();
$ssc = new streaming_context($sc,5);


$topic = array();
$topic["test"]=1;

$kvs = KafkaUtils::createStream($ssc, "localhost:2181", "spark-streaming-consumer",$topic );

$temp = $kvs->flatMap(
    function ($line){
        $temp =  explode(" ",$line);
        return $temp;
    }
);
echo "-----\n";

$temp2 = $temp->map(
    function ($x) {
        return array($x, 1);
    }
);
echo "-----\n";



$temp3 = $temp2 -> reduceByKey(
    function ($x1,$x2) {
        return $x1+$x2;
    }
    ,2
);
echo "-----\n";

$temp3->saveAsTextFiles("/home/gt/php_tmp/");

$ssc->start();
$ssc->awaitTerminationOrTimeout(60000);
echo "php停止了";