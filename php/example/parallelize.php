<?php

$SPARK_HOME = "/home/gt/spark/";
require_once($SPARK_HOME . "/php/src/context.php");

$sc = new context();

#$data = range(1,10);
$rdd = $sc->parallelize(range(1,10),2);
$rs = $rdd->collect();
foreach($rs as $e){
    print($e);
}

$sc->stop();