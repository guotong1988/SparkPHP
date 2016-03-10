<?php require_once("../lib/Java.inc");


class php_call_java {

    var $SparkConf;
    function php_call_java()
    {
        echo "jjjjjj";
        $this->SparkConf = new java("org.apache.spark.SparkConf");
        echo "hhhhhh";
    }
}