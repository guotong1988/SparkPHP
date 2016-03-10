<?php
$temp = __FILE__;
$spark_php_home = substr($temp,0,strrpos($temp,"/")-3);
require_once($spark_php_home."lib/Java.inc");


class php_call_java {
    var $JavaSparkContext;
    var $SparkConf;
    function php_call_java()
    {
        echo "php_call_java构造方法（开始）";
        $this->SparkConf = new java("org.apache.spark.SparkConf");
        $this->JavaSparkContext = new java("org.apache.spark.api.java.JavaSparkContext");
        echo "php_call_java构造方法（结束）";
    }
}