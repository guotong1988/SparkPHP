<?php
$SPARK_HOME = "/home/gt/spark/";
require($SPARK_HOME . "/php/src/context.php");
require($SPARK_HOME . "/php/src/streaming/streaming_context.php");
include_once($SPARK_HOME."/php/src/report_error.php");


$sc = new context();
$ssc = new streaming_context($sc,1);


$topic = array();
$topic["test"]=1;

$kvs = KafkaUtils::createStream($ssc, "localhost:2181", "spark-streaming-consumer",$topic );

$lines = $kvs->map(
    function($x){
       return $x[1];
    }
);

$temp = $lines->flatMap(
    function ($line){
        $temp =  explode(" ",$line);
        return $temp;
    }
);
echo "-----";

$temp2 = $temp->map(
    function ($x) {
        return array($x, 1);
    }
);
echo "-----";



$temp3 = $temp2 -> reduceByKey(
    function ($x1,$x2) {
        return $x1+$x2;
    }
    ,2
);
echo "-----";


$temp3->saveAsTextFiles("/home/gt/php_tmp/");

$ssc->start();
$ssc->awaitTerminationOrTimeout(60000);
echo "php停止了";