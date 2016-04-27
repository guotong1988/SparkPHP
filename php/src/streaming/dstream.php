<?php

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
    if($numPartitions=null) {
        $numPartitions = $this->sc->defaultParallelism;
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
    $func = function ($rdd) use ($createCombiner, $mergeValue, $mergeCombiners,$numPartitions){
        return $rdd->combineByKey($createCombiner, $mergeValue, $mergeCombiners, $numPartitions);
    };
    return $this->transform($func);
}

function mapPartitionsWithIndex($f, $preservesPartitioning=False)
{
    return $this->transform(
        function($rdd) use ($f,$preservesPartitioning) {
            $rdd->mapPartitionsWithIndex($f, $preservesPartitioning);
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

    $saveAsTextFile = function ($t,$rdds) use ($rddToFileName,$prefix,$suffix){
        $path = $rddToFileName($prefix,$suffix,$t);
        foreach($rdds as $rdd) {
            $rdd->saveAsTextFile($path);
        }
    };

    $this->foreachRDD($saveAsTextFile);
}


function foreachRDD($func)
{
    $jfunc = new TransformFunction($this->sc,$func, $this->jrdd_deserializer);
    $temp =  java_closure($jfunc,null,java("org.apache.spark.streaming.api.php.PhpTransformFunction"));

    $api = $this-> ssc ->php_call_java-> PhpDStream;
    $api -> callForeachRDD($this->jdstream, $temp);

}


}


class TransformedDStream extends DStream{

    var $func;
    var $prev;

function __construct($prev,$func){
    $this->ssc = $prev->ssc;
    $this->sc=$this->ssc->sc;
    $this->jrdd_deserializer = $this->sc->serializer;
    $this->is_cached= False;
    $this->is_checkpointed = False;

    if($prev instanceof TransformedDStream && !$prev -> is_cached && !$prev->is_checkpointed){
        echo " 是 ";
        $prev_func = $prev -> func;
        $this->func = function($t,$rdd) use ($func,$prev_func){
          return $func($t,$prev_func($t,$rdd));
        };
        $this->prev = $prev->prev;
    }else{
        echo " 不是 ";
        $this->prev = $prev;
        $this->func = $func;
    }

    $jfunc = new TransformFunction($this->sc,$this->func,$this->prev->jrdd_deserializer);

    $temp =  java_closure($jfunc,null,java("org.apache.spark.streaming.api.php.PhpTransformFunction"));

    $dstream = $this->sc->php_call_java->PhpTransformedDStream($this->prev->jdstream->dstream(), $temp);
    $this->jdstream = $dstream->asJavaDStream();

}


}