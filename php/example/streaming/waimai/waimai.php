<?php
$SPARK_HOME = "/home/gt/spark/";
require($SPARK_HOME . "/php/src/context.php");
require($SPARK_HOME . "/php/src/streaming/streaming_context.php");
#include_once($SPARK_HOME."/php/src/report_error.php");
#usage:  ./spark-submit --jars /home/gt/spark/external/kafka-assembly/target/spark-streaming-kafka-assembly_2.10-1.6.0.jar kafka_wordcount.php

$sc = new context();
$ssc = new streaming_context($sc,5);


$topic = array();
$topic["test"]=1;

$kvs = KafkaUtils::createStream($ssc, "localhost:2181", "spark-streaming-consumer",$topic );

$temp = $kvs->map(
    function ($x) {

        $detail = CommonUtil::parseMessageJson($x);
        $cleaner = new DataCleaner();
        if ($cleaner->isDirty($detail)) {
            return "DIRTY";
        }

        $temp = date("Y-m-d h:i:s", $detail->time);
        $year = substr($temp,0,4);
        $month = substr($temp,5,2);
        $day = substr($temp,8,2);
        $hour = substr($temp,10,2);
        $min = substr($temp,14,2);
        $second = 0;
        $re = mktime($hour,$min,$second,$month,$day,$year);


        return $detail->aoiId."$".$detail->dataState."$".$re;
    }
);
echo "-----\n";



$temp2 = $temp -> reduceByKey(
    function ($x1,$x2) {
        return $x1+$x2;
    }
    ,2
);
echo "-----\n";

$temp2->foreachRDD(
    function($rdd){
        $re = $rdd->collect();
        $produce = \Kafka\Produce::getInstance('localhost:2181', 3000);
        $produce->setMessages("topic",0,$re);
        $produce->send();
    }
);


$ssc->start();
$ssc->awaitTerminationOrTimeout(60000);
echo "php停止了";