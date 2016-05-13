<?php
$SPARK_HOME = "/home/gt/spark/";
require_once($SPARK_HOME . "/php/src/context.php");
require_once($SPARK_HOME . "/php/src/streaming/streaming_context.php");
include_once($SPARK_HOME."/php/src/report_error.php");

$sc = new context();
$ssc = new streaming_context($sc,10);
$lines = $ssc->textFileStream("hdfs://127.0.0.1:9000/test/");

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