<?php
$SPARK_HOME = "/home/gt/spark/";
require($SPARK_HOME . "/php/src/context.php");

$sc = new context();

#$data = range(1,10);
$rdd = $sc->text_file("/home/gt/wordcount.txt",2);
$iter = $rdd->take(50);
print_r($iter);

$sc->stop();