<?php
$temp = __FILE__;
$spark_php_home = substr($temp,0,strrpos($temp,"/")-3);
require_once($spark_php_home."lib/Java.inc");


class php_call_java {
    var $JavaSparkContext;
    var $SparkConf;
    var $PhpRDD;
    var $Byte;
    var $PhpAccumulatorParam;
    function php_call_java()
    {
        $this->SparkConf = new java("org.apache.spark.SparkConf");
        $this->JavaSparkContext = new java("org.apache.spark.api.java.JavaSparkContext");
        $this->PhpRDD = new java("org.apache.spark.api.php.PhpRDD");
    }

    function php_accumulator_param($host,$port){
        $this->PhpAccumulatorParam = new java("org.apache.spark.api.php.PhpAccumulatorParam",$this->new_java_string($host),$port);
        return $this->PhpAccumulatorParam;
    }

    function BytesToString(){
        return new java("org.apache.spark.api.php.BytesToString");
    }

    function new_java_string($php_string){
        return new java("java.lang.String",$php_string);
    }


    function new_java_list(){
        return new java("java.util.ArrayList");
    }

    function new_java_map(){
        return new java("java.util.HashMap");
    }

    function pair_wise_rdd($rdd){
        return new java("org.apache.spark.api.php.PairwiseRDD",$rdd);
    }

    function php_partitioner($numPartitions,
                             $partitionFunc){
        return new java("org.apache.spark.api.php.PhpPartitioner",$numPartitions,$partitionFunc);
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

       # echo gettype($serialized_cmd)."!!!!!";
       # $temp_string = "";
       # foreach($serialized_cmd as $key=>$value){
       #     $temp_string+="##"+$value;
       # }
       # $temp_byte_array = unpack('C*', $serialized_cmd);#string转byte
       # echo gettype($temp_byte_array)."?????";
        $preservesPartitioning = new java("java.lang.Boolean",$preservesPartitioning);
        $phpExec=$this->new_java_string($phpExec);
        $phpVer= $this->new_java_string($phpVer);

        $this->PhpRDD = new java("org.apache.spark.api.php.PhpRDD",
            $prev_jrdd,
            $serialized_cmd,
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