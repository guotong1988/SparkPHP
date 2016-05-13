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


$temp3 = $temp2 -> reduceByKey(
    function ($x1,$x2) {
        return $x1+$x2;
    }
);

foreach($temp3->collect() as $key=>$element){
    echo $element;
    foreach($element as $ele){
        echo $ele." ";

    }
    echo "!!!\n";
};

$sc->stop();

