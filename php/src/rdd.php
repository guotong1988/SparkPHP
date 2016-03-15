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


    function convert_list($php_list,$sc)
    {
        $jlist = $sc->jvm->new_java_list();
        foreach ($php_list as $key => $value) {
            $jlist->add($value);
        }
        return $jlist;
    }


    function convert_map($php_map,$sc)
    {
        $jmap = $sc->jvm->new_java_map();
        foreach ($php_map as $key => $value) {
            $jmap[$key]->put($key,$value);
        }
        return $jmap;
    }

    function prepare_for_python_RDD($sc, $command, $obj=null){
        # the serialized command will be compressed by broadcast
        $pickled_command = serialize($command);
        if(strlen($pickled_command) > (1 << 20)) {  # 1M
            # The broadcast will have same life cycle as created PythonRDD
            $broadcast = $sc -> broadcast($pickled_command);
            $pickled_command = serialize($broadcast);
        }

        $temp = array();
        for($i=0;$i<sizeof($sc->pickled_broadcast_vars);$i++){
            array_push($temp,$sc->pickled_broadcast_vars[$i]);
        }
        $broadcast_vars = $this->convert_list($temp,$sc);
        $sc->pickled_broadcast_vars->clear();
        $env = $this->convert_map($sc->environment,$sc);
        $includes =$this-> convert_list($sc->python_includes, $sc);
        return array($pickled_command, $broadcast_vars, $env, $includes);
    }
}

class pipelined_rdd extends rdd{

    var $func;
    var $preservesPartitioning;
    var $prev_jrdd;
    var $prev_jrdd_deserializer;
    var $prev_func;
    var $ctx;
    var $jrdd_val;
    var $jrdd_deserializer;
    var $bypass_serializer;
    var $partitioner;
    var $jrdd;
    function pipeline_func($split, $iterator){
        return func($split, $this->prev_func($split, $iterator));
    }
    function  pipelined_rdd(rdd $prev_rdd,callable $func, $preservesPartitioning=False) {
        if(!($prev_rdd instanceof pipelined_rdd) || !$prev_rdd.is_pipelinable()) {
            # This transformation is the first in its stage:
            $this->func = $func;
            $this->preservesPartitioning = $preservesPartitioning;
            $this->prev_jrdd = $prev_rdd->jrdd;
            $this->prev_jrdd_deserializer = $prev_rdd->jrdd_deserializer;
        }else {
            $this->prev_func = $prev_rdd->func;
            $this->func = 'pipeline_func';
            $this->preservesPartitioning = $prev_rdd->preservesPartitioning && $preservesPartitioning;
            $this->prev_jrdd = $prev_rdd->prev_jrdd; # maintain the pipeline
            $this->prev_jrdd_deserializer = $prev_rdd->prev_jrdd_deserializer;
        }
        $this -> is_cached = False;
        $this -> is_checkpointed = False;
        $this -> ctx = $prev_rdd -> ctx;
        $this -> prev_jrdd = $prev_rdd;
        $this -> jrdd_val = null;
        $this -> id = null;
        $this -> jrdd_deserializer = $this -> ctx -> serializer;
        $this -> bypass_serializer = False;
        if($this->preservesPartitioning){
            $this -> partitioner = $prev_rdd->partitioner;
        }else {
            $this -> partitioner = null;
        }


        if($this->jrdd_val){
            $this->jrdd = $this->jrdd_val;
        }
        if($this->bypass_serializer) {
#            $this->jrdd_deserializer = new NoOpSerializer();
        }
        $profiler=null;
        if($this->ctx->profiler_collector){
            $profiler = $this->ctx->profiler_collector->new_profiler($this->ctx);
        } else {
            $profiler = null;
        }
        $command = array();
        $command[0] = $this-> func;
        $command[1] = $profiler;
        $command[2] = $this->prev_jrdd_deserializer;
        $command[3] = $this->jrdd_deserializer;
        $tempArray = $this->prepare_for_python_RDD($this->ctx, $command, $this);
        $pickled_cmd= $tempArray[0];
        $bvars= $tempArray[1];
        $env= $tempArray[2];
        $includes = $tempArray[3];
        $python_rdd = $this->ctx->jvm->PythonRDD(
                $this->prev_jrdd->rdd(),
                $pickled_cmd,
                $env, $includes, $this->preservesPartitioning,
                $this->ctx->pythonExec,
                $this->ctx->pythonVer,
                $bvars,
                $this->ctx->javaAccumulator);
        $this->jrdd_val = $python_rdd->asJavaRDD();
        if($profiler) {
            $this->id = $this->jrdd_val->id();
            $this->ctx->profiler_collector->add_profiler($this->id, $profiler);
            return $this->jrdd_val;
        }
    }

    function is_pipelinable(){
        return !($this->is_cached || $this->is_checkpointed);
    }
}

