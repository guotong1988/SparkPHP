<?php

#usage：在命令行 nc -lk 9999 来输入数据

$SPARK_HOME = "/home/gt/spark/";
require_once($SPARK_HOME . "/php/src/context.php");
require_once($SPARK_HOME . "/php/src/streaming/streaming_context.php");
include_once($SPARK_HOME . "/php/src/report_error.php");

$sc = new context();
$ssc = new streaming_context($sc,10);
$ssc->checkpoint("/home/gt/php_tmp/");

$lines = $ssc->socketTextStream("localhost","9999");

$temp = $lines->flatMap(
    function ($line){
        $temp =  explode(" ",$line);
        return $temp;
    }
);
echo "-----";

$temp2 = $temp->map(
    function ($x) {
        return array($x, 1);
    }
);
echo "-----";



$temp3 = $temp2 -> updateStateByKey(
    function ($new_values, $last_sum){#$new_value是一个数组，$last_sum可能是first_value

        file_put_contents("/home/".get_current_user()."/php_worker17.txt", var_export($new_values,TRUE)."!!!\n", FILE_APPEND);
        file_put_contents("/home/".get_current_user()."/php_worker17.txt", $last_sum."===\n", FILE_APPEND);

        if($last_sum==null){
            $last_sum=0;
        }

        return array_sum($new_values)+$last_sum;
    }
    ,1
);
echo "-----";


$temp3->saveAsTextFiles("/home/gt/php_tmp3/".time());

$ssc->start();
$ssc->awaitTerminationOrTimeout(600000);
echo "php停止了";