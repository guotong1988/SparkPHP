<?php
$SPARK_HOME = "/home/gt/spark/";
require($SPARK_HOME . "/php/src/context.php");

$sc = new context();
$ssc = new streaming_context($sc,1);
$lines = $ssc->textFileStream("hdfs:/127.0.0.1:9000/test/");

$temp = $lines->flatMap(
    function ($line){
        $temp =  explode(" ",$line);
        return $temp;
    }
);

$temp2 = $temp->map(
    function ($x) {
        return array($x, 1);
    }
);

$temp3 = $temp2 -> reduceByKey(
    function ($x1,$x2) {
        return $x1+$x2;
    }
);

$ssc->start();
$ssc->awaitTermination();
