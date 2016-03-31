<?php
$SPARK_HOME = "/home/gt/spark/";
require($SPARK_HOME . "/php/src/context.php");

$sc = new context();

#$data = range(1,10);
$rdd = $sc->text_file("/home/gt/groupbykey.txt",2);
$temp = $rdd->keyBy(
  function ($x){
      return strlen($x);
  }
);
$result= $temp->groupByKey()->collect();
foreach($result as $element) {
    echo $element;
    echo "!!!\n";
}