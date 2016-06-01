<?php
$SPARK_HOME = "/home/gt/spark/";
require_once($SPARK_HOME . "/php/src/context.php");

$sc = new context();

$rdd = $sc->text_file("/home/gt/word.txt",2);
$rdd2 = $sc->text_file("/home/gt/word2.txt",2);

$rdd = $rdd->keyBy(
    function($row){
        return strlen($row);
    }
);

$rdd2 = $rdd2->keyBy(
    function($row){
        return strlen($row);
    }
);
$rdd3 = $rdd2->groupbyKey(2);
#$rdd4 = $rdd3->groupbyKey2(2)->collect();



//foreach($rdd3 as $k=>$e){
//    print_r($e);
//}

$re = $rdd3 -> cogroup($rdd,2)->collect();;

foreach($re as $k=>$e){
    print_r($e);
}

$sc->stop();

