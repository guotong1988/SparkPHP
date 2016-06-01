<?php
$SPARK_HOME = "/home/gt/spark/";
require($SPARK_HOME . "/php/src/context.php");
include_once($SPARK_HOME."/php/src/report_error.php");
$sc = new context();

$temp4 = $sc->text_file("/home/gt/result/part-00000",2);


foreach($temp4->collect() as $key=>$value){
    echo $value;
    echo "!!!\n";
};

