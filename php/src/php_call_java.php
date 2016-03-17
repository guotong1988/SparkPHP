<?php
$temp = __FILE__;
$spark_php_home = substr($temp,0,strrpos($temp,"/")-3);
require_once($spark_php_home."lib/Java.inc");


class php_call_java {
    var $JavaSparkContext;
    var $SparkConf;
    var $PhpRDD;
    var $Byte;
    function php_call_java()
    {
        echo "php_call_java构造方法（开始）";
        $this->SparkConf = new java("org.apache.spark.SparkConf");
        $this->JavaSparkContext = new java("org.apache.spark.api.java.JavaSparkContext");


        echo "php_call_java构造方法（结束）";
    }

    function new_java_list(){
        return new java("java.util.ArrayList");
    }

    function new_java_map(){
        return new java("java.util.HashMap");
    }

    function phpRDD(
                $prev_jrdd,
                $serialized_cmd,
                $env,
                $includes,
                $preservesPartitioning,
                $phpExec,
                $phpVer,
                $bvars,
                $javaAccumulator){
        $temp = array();
        foreach($serialized_cmd as $key=>$value){
            $this->Byte = new java("java.lang.Byte",$value);
            $temp[$key]=$this->Byte;
        }

        $preservesPartitioning = new java("java.lang.Boolean",$preservesPartitioning);

        echo gettype($temp);

        $this->PhpRDD = new java("org.apache.spark.api.php.PhpRDD",
            $prev_jrdd,
            $temp,
            $env,
            $includes,
            $preservesPartitioning,
            $phpExec,
            $phpVer,
            $bvars,
            $javaAccumulator);

        return $this->PhpRDD;
    }

}