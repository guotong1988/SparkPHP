<?php
$SPARK_HOME = "/home/gt/spark/";
require_once($SPARK_HOME . "/php/src/context.php");
include_once($SPARK_HOME."/php/src/report_error.php");
$sc = new context();


$lines = $sc->text_file("/home/gt/wordcount.txt",2);
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

$temp4 = $temp3 -> map(#去掉这行输出是array->toString()
    function ($x) {
        $re = "";
        foreach($x as $e){
            $re = $re." ".$e;
        }
        return $re;
    }
);


$temp4->saveAsTextFile("/home/gt/result");
