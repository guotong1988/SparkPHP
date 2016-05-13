<?php
$SPARK_HOME = "/home/gt/spark/";
require_once($SPARK_HOME . "/php/src/context.php");
#include_once($SPARK_HOME."/php/src/report_error.php");
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

foreach($temp2->collect() as $value){
    foreach($value as $ele){
        echo $ele." ";
    }
    echo "\n";
};
/*

$temp3 = $temp2 -> reduceByKey(
    function ($x1,$x2) {
        return $x1+$x2;
    }
);

foreach($temp3->collect() as $key=>$value){
    echo $key;
    echo " ";
    echo $value;
    echo "!!!\n";
};
*/