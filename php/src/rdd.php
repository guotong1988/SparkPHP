<?php

$ADD = 1;
$temp = __FILE__;
$spark_php_home = substr($temp,0,strrpos($temp,"/")-3);
require($spark_php_home . "src/my_iterator.php");
require($spark_php_home . "src/sock_input_stream.php");
class rdd {

    var $jrdd;
    var $is_cached;
    var $is_checkpointed;
    var $ctx;
    var $deserializer;
    var $id;
    var $partitioner;
    function rdd($jrdd,$ctx,$deserializer) {
        $this->jrdd = $jrdd;
        $this->is_cached = False;
        $this->is_checkpointed = False;
        $this->ctx = $ctx;
        $this->deserializer = $deserializer;
        $this->id = $jrdd->id();
        $this->partitioner = null;
    }

    function f0($iterator){
        $count = 0;
        foreach($iterator as $element) {
            $count++;
        }
        return $count;
    }

    function count()
    {
        $function_name='f0';
        return $this->mapPartitions($function_name)->sum();
    }

    function f1($split, $iterator){
        $f = $this->mapPartitions_f;
        return $f($iterator);
    }

    var $mapPartitions_f;

    function mapPartitions(callable $f, $preservesPartitioning=False)
    {
        $function_name='f1';
        $this->$mapPartitions_f=$f;
        return $this->mapPartitionsWithIndex($function_name, $preservesPartitioning);
    }

    function mapPartitionsWithIndex(callable $f, $preservesPartitioning=False)
    {
        return new pipelined_rdd($this, $f, $preservesPartitioning);
    }

    function f2($iterator){
        return array_sum($iterator);
    }

#        >>> sc.parallelize([1.0, 2.0, 3.0]).sum()
#        输出6.0
    function sum()
    {
        $function_name='f2';
        global $ADD;
        return $this->mapPartitions($function_name)->fold(0, $ADD);
    }

    function f3($iterator,$zeroValue=0,$operator=1){
        $acc = $zeroValue;
        foreach($iterator as $element) {
            global $ADD;
            if($operator==$ADD) {
                $acc = $element + $acc;
            }
        }
        $temp = array();
        array_push($temp,$acc);
        return new my_iterator($temp);
    }

    function fold($zeroValue, $op){
        $function_name='f3';
        $temp = $this->mapPartitions($function_name)->collect();
        global $ADD;
        if($op == $ADD){
            $op = 'add_function';
        }
        return array_reduce($temp->get_array(),$op,$zeroValue);
    }

    function add_function($v0,$v1){
        return $v0+$v1;
    }

    function collect()
    {
    #    with SCCallSiteSync(self . context) as css:
        $port = $this->ctx->jvm->PhpRDD->collectAndServe($this->jrdd->rdd());
        return $this->load_from_socket($port,$this->deserializer);
    }


    function load_from_socket($port,serializers $deserializer)
    {
        $sock = socket_create ( AF_INET, SOCK_STREAM, SOL_TCP );
        if ($sock == false) {
            echo "socket_create()失败:" . socket_strerror(socket_last_error($sock)) . "\n";
        }else {
            echo "socket_create()成功\n";
        }
        $result =socket_connect ( $sock, '127.0.0.1', 18081 );
        if ($result == false) {
            echo "socket_connect()失败:" . socket_strerror(socket_last_error($sock)) . "\n";
        }else {
            echo "socket_connect()成功\n";
        }
        $stream = new sock_input_stream($sock);
        $item_array = $deserializer->load_stream($stream);
        socket_close($sock);
        return new my_iterator($item_array);
    }

    function f4(my_iterator $iterator,$f){
        try {
            $initial = $iterator->first();
        }catch(Exception $e) {
            return null;
        }
        return array_reduce($iterator->get_array(),$f,$initial);
    }

    var $reduce_f;

    function reduce($f){
        $function_name = 'f4';
        $this->$reduce_f=$f;
        $temp = $this->mapPartitions($function_name)->collect();
        if($temp!=null){
            return array_reduce($f, $temp->get_array());
        } else {
            throw new Exception("Can not reduce() empty RDD");
        }
    }
}

class pipelined_rdd extends rdd{

    function  pipelined_rdd($prev_rdd,callable $func, $preservesPartitioning=False) {

    }
}

