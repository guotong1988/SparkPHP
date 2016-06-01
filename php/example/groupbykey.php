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
/*
foreach($temp->collect() as $element) {
    echo $element."? ";
    foreach ($element as $e) {
        echo " ";
        echo $e;
    }
    echo "!!!\n";
}

*/
$result= $temp->groupByKey()->collect();
foreach($result as $element) {
    foreach($element as $ele){
        echo " ??";
        echo $ele;
        if(is_array($ele)){
            foreach($ele as $e){
                echo " ".$e;
            }
        }
    }
    echo "!!!\n";
}
