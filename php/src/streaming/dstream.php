<?php

/*
$spark_php_home = substr(__DIR__,0,strrpos(__DIR__,"/")-3);
require $spark_php_home.'/src/vendor/autoload.php';
use SuperClosure\Serializer;
*/

class dstream{

    var $jdstream;
    var $ssc;
    var $sc;
    var $jrdd_deserializer;
    var $is_cached;
    var $is_checkpointed;

function __construct($jdstream, $ssc, $jrdd_deserializer)
{
    $this->jdstream = $jdstream;
    $this->ssc = $ssc;
    $this->sc = $ssc->sc;
    $this->jrdd_deserializer = $jrdd_deserializer;
    $this->is_cached = False;
    $this->is_checkpointed = False;
}


function map($f, $preservesPartitioning=False)
{
    $is_list = function ($arr){
        return is_array($arr) && ($arr == array() || array_keys($arr) === range(0,count($arr)-1) );
    };

    $func = function ($iterator)use ($f,$is_list){
        if($iterator instanceof Generator){
            $re = array();
            foreach($iterator as $e){
                if($e!="") {
                    array_push($re, $f($e));
                }
            }
            return $re;
        }elseif(is_array($iterator)&&sizeof($iterator)==0){
            return array();
        }elseif($is_list($iterator)) {
            return array_map($f, $iterator);
        }else {
            $re = array();
            foreach ($iterator as $k => $v) {
                $temp = array();
                array_push($temp, $k);
                array_push($temp, $v);
                array_push($re, $f($temp));
            }
            return $re;
        }
    };

    return $this->mapPartitions($func, $preservesPartitioning);
}


function mapPartitions($f, $preservesPartitioning=False)
{
    $func = function ($s, $iterator)use ($f) {
        return $f($iterator);
    };
    return $this->mapPartitionsWithIndex($func, $preservesPartitioning);
}

function flatMap($f, $preservesPartitioning=False)
{
    $func = function ($split, $iterator) use ($f){


        $sub_is_array = False;
        foreach($iterator as $key=>$value){
            $temp = $f($value);
            if(is_array($temp)){
                $sub_is_array = True;
                break;
            }
        }

        if($sub_is_array){
            $result = array();

            foreach($iterator as $key=>$value){
                $temp = $f($value);
                if(is_array($temp)){
                    foreach($temp as $e){
                        array_push($result,$e);
                    }
                }
            }
            return $result;
        }else{
            $result = array();
            foreach($iterator as $key=>$value){
                $temp = $f($value);
                array_push($result,$temp);
            }
            return $result;
        }
    };
    return $this->mapPartitionsWithIndex($func, $preservesPartitioning);
}



function reduceByKey($func, $numPartitions=null)
{
    if($numPartitions==null) {
        $numPartitions = 2;#大坑啊 $this->sc->defaultParallelism;
    }
    return $this->combineByKey(
        function ($x){
            return $x;
        }, $func, $func, $numPartitions);
}



function combineByKey($createCombiner, $mergeValue, $mergeCombiners, $numPartitions=null)
{
    if($numPartitions==null) {
        $numPartitions = $this->sc->defaultParallelism;
    }

    $partitionFunc = function ($x) {
        if ($x == null) {
            return 0;
        }
        if (is_array($x)) {
            $h=0;
            foreach($x as $ele){
                $h ^= hexdec(hash("md5", $ele));
            }
            return $h;
        }
        return hexdec(hash("md5", $x));#http://stackoverflow.com/questions/3379471/php-number-only-hash
    };

    $func = function ($t,$rdd) use ($createCombiner, $mergeValue, $mergeCombiners,$numPartitions,$partitionFunc){
        return $rdd->combineByKey($createCombiner, $mergeValue, $mergeCombiners, $numPartitions,$partitionFunc);
    };
    return $this->transform($func);
}

function mapPartitionsWithIndex($f, $preservesPartitioning=False)
{
    return $this->transform(
        function($t,$rdd) use ($f,$preservesPartitioning) {
            return $rdd->mapPartitionsWithIndex($f, $preservesPartitioning);
        }
    );
}


function transform($func)
{
    return new TransformedDStream($this,$func);
}



function pprint($num=10)
{

#        Print the first num elements of each RDD generated in this DStream.
#        @param num: the number of elements from the first will be printed.

    $takeAndPrint = function ($time, $rdd)use ($num) {
        $taken = $rdd->take($num + 1);
        print("-------------------------------------------");
        print("Time: %s" % $time);
        print("-------------------------------------------");
        for($i=0;$i<$num;$i++){
            print($taken[$i]);
        }
    };
    $this->foreachRDD($takeAndPrint);
}


function saveAsTextFiles($prefix,$suffix=null){

    $rddToFileName = function ($prefix,$suffix,$timestamp){
        if($suffix==null) {
            return $prefix . "-" . $timestamp;
        } else {
            return $prefix . "-" . $timestamp . "-" . $suffix;
        }
    };

    $saveAsTextFile = function ($t,$rdd) use ($rddToFileName,$prefix,$suffix){
        $path = $rddToFileName($prefix,$suffix,$t);

 //       file_put_contents("/home/".get_current_user()."/php_printer1111.txt", gettype($rdd)."---!!!???\n", FILE_APPEND);
//        if($rdd!=null ){
//            $rdd->saveAsTextFile($path);
//        }else{
            $rdd->saveAsTextFile($path);
//        }


    };

    return $this->foreachRDD($saveAsTextFile);
}


function foreachRDD($func)
{
    $jfunc = new TransformFunction($this->sc,$func, $this->jrdd_deserializer);
    $temp =  java_closure($jfunc,null,java("org.apache.spark.streaming.api.php.PhpTransformFunction"));

    $api = $this-> ssc ->php_call_java-> PhpDStream;
    return $api -> callForeachRDD($this->jdstream, $temp);

}


function updateStateByKey($updateFunc, $numPartitions=null){

    if($numPartitions==null) {
        $numPartitions = 2;  #self . _sc . defaultParallelism
    }

    $reduceFunc = function($t,$a,$b) use ($numPartitions,$updateFunc) {
        file_put_contents("/home/".get_current_user()."/php_printer111.txt", gettype($a)."---!!!???\n", FILE_APPEND);
        file_put_contents("/home/".get_current_user()."/php_printer111.txt", gettype($b)."$$$!!!???\n\n", FILE_APPEND);
//        file_put_contents("/home/".get_current_user()."/php_printer111.txt", var_export(java_is_null($b),TRUE)."===!!!???", FILE_APPEND);
//        file_put_contents("/home/".get_current_user()."/php_printer111.txt", var_export(java_is_true($b),TRUE)."===!!!???\n\n\n\n", FILE_APPEND);
            if ($a == null) {
//                $b->saveAsTextFile("/home/gt/php_tmp25/");
                $g = $b->groupbyKey($numPartitions);
//                $g->saveAsTextFile("/home/gt/php_tmp26/");
                $state = $g->mapValues(
                    function ($everyValue) use ($updateFunc) {

//                        file_put_contents("/home/" . get_current_user() . "/php_worker888.txt", var_export($everyValue,TRUE) . "---!!!\n", FILE_APPEND);

                        if(!is_array($everyValue)){
                            $temp = array();
                            array_push($temp,$everyValue);
                            return $updateFunc($temp, null);
                        }

                        return $updateFunc($everyValue, null);
                    }
                );
//                $state->saveAsTextFile("/home/gt/php_tmp27/");
                return $state;
            } else {
//                $a->saveAsTextFile("/home/gt/php_tmp23/".time());
//                $b->saveAsTextFile("/home/gt/php_tmp24/");
                $g = $a->cogroup($b, $numPartitions);#合并老数据和新数据

//                $g->saveAsTextFile2("/home/gt/php_tmp21/".time());

                $state = $g->mapValues(
                    function ($everyValue) use ($updateFunc) {

                        if(is_array($everyValue)) {

                            #   file_put_contents("/home/" . get_current_user() . "/php_worker119.txt", var_export($everyValue, true) . "!!!\n", FILE_APPEND);
//                            file_put_contents("/home/" . get_current_user() . "/php_worker999.txt", var_export($everyValue, TRUE) . "---!!!\n", FILE_APPEND);
                            $temp = array();
                            for ($i = 1; $i < sizeof($everyValue); $i++) {
                                array_push($temp, $everyValue[$i]);
                            }

                            return $updateFunc($temp, $everyValue[0]);//[0]是cogroup的老数据
                        }else{
                            return $updateFunc(array(), $everyValue);
                        }

                    }
                );
//                $state->saveAsTextFile2("/home/gt/php_tmp22/".time());
                return $state;

            }
    };

    $jreduceFunc =new TransformFunction($this->sc,$reduceFunc,new utf8_deserializer());#TODO

    dstream::$theFunc = $reduceFunc;

    $temp = java_closure($jreduceFunc, null, java("org.apache.spark.streaming.api.php.PhpTransformFunction"));

    $dstream = $this->sc->php_call_java->PhpStateDStream($this->jdstream->dstream(), $temp);

    return new DStream($dstream->asJavaDStream(), $this->ssc, new utf8_deserializer());#TODO
}

static $theFunc;

}


class TransformedDStream extends dstream{

    var $func;
    var $prev;

function __construct($prev,$func){
    $this->ssc = $prev->ssc;
    $this->sc=$this->ssc->sc;
    $this->jrdd_deserializer = $this->sc->serializer;
    $this->is_cached= False;
    $this->is_checkpointed = False;

    if($prev instanceof TransformedDStream && !$prev -> is_cached && !$prev->is_checkpointed){
        $prev_func = $prev -> func;
        $this->func = function($t,$rdd) use ($func,$prev_func){
            $temp =  $prev_func($t,$rdd);
            return $func($t,$temp);
        };
        $this->prev = $prev->prev;
    }else{
        $this->prev = $prev;
        $this->func = $func;
    }

    if($this->jdstream==null){
        $jfunc = new TransformFunction($this->sc, $this->func, $this->prev->jrdd_deserializer);

        $temp = java_closure($jfunc, null, java("org.apache.spark.streaming.api.php.PhpTransformFunction"));

        $dstream = $this->sc->php_call_java->PhpTransformedDStream($this->prev->jdstream->dstream(), $temp);
        $this->jdstream = $dstream->asJavaDStream();
    }
}


}