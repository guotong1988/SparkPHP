<?php
$SPARK_HOME = "/home/gt/spark/";
require($SPARK_HOME . "/php/src/context.php");

$sc = new context();


$lines = $sc->text_file("/home/gt/wordcount.txt",2);
$temp = $lines->flatMap(
    function ($line){
        $temp =  explode(" ",$line);
        return $temp;
    }
);
foreach($temp->collect() as $key=>$value){
    echo $value;
    echo "!!!\n";
};
