<?php
$SPARK_HOME = "/home/gt/spark/";
require($SPARK_HOME . "/php/src/context.php");

$sc = new context();


$rdd = $sc->text_file("/home/gt/sum.txt",2);
$iter = $rdd->fold(0,1);
echo $iter."!!!!????";
