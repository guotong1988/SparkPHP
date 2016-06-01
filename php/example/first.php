<?php
$SPARK_HOME = "/home/gt/spark/";
require($SPARK_HOME . "/php/src/context.php");

$sc = new context();

$rdd = $sc->parallelize(range(3,7),2);
$iter = $rdd->first();
print($iter);
print("\n");
$sc->stop();