<?php
$SPARK_HOME = "/home/gt/spark/";
require_once($SPARK_HOME . "/php/src/context.php");

$sc = new context();

$rdd = $sc->text_file("/home/gt/wordcount.txt",2);
$iter = $rdd->count();
echo $iter."!!!!????";
$sc->stop();