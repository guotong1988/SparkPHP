<?php require_once("../lib/Java.inc");


class php_call_java {
    var $JavaSparkContext;
    var $SparkConf;
    function php_call_java()
    {
        echo "php_call_java构造方法（开始）";
        $this->JavaSparkContext = new java("org.apache.spark.api.java.JavaSparkContext");
        $this->SparkConf = new java("org.apache.spark.SparkConf");
        echo "php_call_java构造方法（结束）";
    }
}