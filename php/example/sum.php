<?php
$SPARK_HOME = "/home/gt/spark/";
require($SPARK_HOME . "/php/src/context.php");
include_once($SPARK_HOME."/php/src/report_error.php");
$sc = new context();


$rdd = $sc->text_file("/home/gt/sum.txt",2);
$iter = $rdd->sum();
echo $iter."!!!!????";
