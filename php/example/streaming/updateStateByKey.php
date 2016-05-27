<?php

#usage：在命令行 nc -lk 9999 来输入数据

$SPARK_HOME = "/home/gt/spark/";
require_once($SPARK_HOME . "/php/src/context.php");
require_once($SPARK_HOME . "/php/src/streaming/streaming_context.php");
include_once($SPARK_HOME."/php/src/report_error.php");

$sc = new context();
$ssc = new streaming_context($sc,10);
$lines = $ssc->socketTextStream("localhost","9999");

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



$temp3 = $temp2 -> updateStateByKey(
    function ($new_values, $last_sum){
        return array_sum($new_values) + ($last_sum or 0);
    }
    ,2
);
echo "-----";


$temp3->saveAsTextFiles("/home/gt/php_tmp/");

$ssc->start();
$ssc->awaitTerminationOrTimeout(60000);
echo "php停止了";