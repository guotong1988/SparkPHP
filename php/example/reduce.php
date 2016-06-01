<?php
$SPARK_HOME = "/home/gt/spark/";
require($SPARK_HOME . "/php/src/context.php");

$sc = new context();

#$data = range(1,10);
$rdd = $sc->text_file("/home/gt/reduce.txt",2);
$temp = $rdd->reduce(
    function ($x0,$x1){
        return $x0+$x1;
    }
);
foreach($temp as $element) {
    echo $element;
    echo "!!!\n";
}