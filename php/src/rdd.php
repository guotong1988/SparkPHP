<?php


$temp = __FILE__;
$spark_php_home = substr($temp,0,strrpos($temp,"/")-3);
require($spark_php_home . "src/my_iterator.php");
require($spark_php_home . "src/sock_input_stream.php");
require 'vendor/autoload.php';
use SuperClosure\Serializer;

class rdd {

    var $jrdd;
    var $is_cached;
    var $is_checkpointed;
    var $ctx;
    var $deserializer;
    var $id;
    var $partitioner;
    var $s;
    function rdd($jrdd,$ctx,$deserializer) {
        $this->jrdd = $jrdd;
        $this->is_cached = False;
        $this->is_checkpointed = False;
        $this->ctx = $ctx;
        $this->deserializer = $deserializer;
        $this->id = $jrdd->setName("!!!");
        $this->partitioner = null;
        $this->s=new Serializer();
    }

    function id(){
        return $this->id;
    }

    function count()
    {

        return  $this->mapPartitions(

            function ($iterator){
                $count = 0;
                foreach($iterator as $element) {
                    $count++;
                }
                return $count;
            }

        )->sum();

    }

    function mapPartitions(callable $f, $preservesPartitioning=False)
    {
        return $this->mapPartitionsWithIndex(

            function ($split, $iterator) use ($f){
                return $f($iterator);
            }

            , $preservesPartitioning);
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
        $ADD = 1;
        return $this->mapPartitions(
            function($iterator){
                return array_sum($iterator);
            }
        )->fold(0, $ADD);
    }

    function fold($zeroValue, $op){
        $temp = $this->mapPartitions(

            function ($iterator) use($zeroValue,$op){
                $acc = $zeroValue;
                foreach($iterator as $element) {
                    $ADD = 1;
                    if($op==$ADD) {
                        $acc = $element + $acc;
                    }
                }
                $temp = array();
                array_push($temp,$acc);
                return new my_iterator($temp);
            }

        )->collect();
        $ADD = 1;
        if($op == $ADD){
            $op = 'add_function';
        }
        return array_reduce($temp->get_array(),

            function ($v0,$v1){
                return $v0+$v1;
            }

            ,$zeroValue);
    }



    function collect()
    {
    #    with SCCallSiteSync(self . context) as css:
        $port = $this->ctx->php_call_java->PhpRDD->collectAndServe($this->jrdd->rdd());
        return $this->load_from_socket($port,$this->deserializer);
    }


    function load_from_socket($port,$deserializer)
    {
        $sock = socket_create ( AF_INET, SOCK_STREAM, SOL_TCP );
        if ($sock == false) {
            echo "socket_create()失败:" . socket_strerror(socket_last_error($sock)) . "\n";
        }else {
            echo "socket_create()成功\n";
        }
        $result =socket_connect ( $sock, '127.0.0.1',18081);
        if ($result == false) {
            echo "socket_connect()失败:" . socket_strerror(socket_last_error($sock)) . "\n";
        }else {
            echo "socket_connect()成功\n";
        }
        $stream = new sock_input_stream($sock);
        if($deserializer==null){
            $deserializer = new utf8_deserializer();
        }
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
        $temp = $this->mapPartitions(

            function (my_iterator $iterator) use ($f){
                try {
                    $initial = $iterator->first();
                }catch(Exception $e) {
                    return null;
                }
                return array_reduce($iterator->get_array(),$f,$initial);
            }

        )->collect();
        if($temp!=null){
            return array_reduce($temp->get_array(),$f);
        } else {
            throw new Exception("Can not reduce() empty RDD");
        }
    }


    function convert_list($php_list,$sc)
    {
        $jlist = $sc->php_call_java->new_java_list();
        foreach ($php_list as $key => $value) {
            $jlist->add($value);
        }
        return $jlist;
    }


    function convert_map($php_map,$sc)
    {
        $jmap = $sc->php_call_java->new_java_map();
        foreach ($php_map as $key => $value) {
            $jmap[$key]->put($key,$value);
        }
        return $jmap;
    }

    function prepare_for_python_RDD($sc, $command, $obj=null){
        # the serialized command will be compressed by broadcast
        if($this->s==null) {
            $this->s=new Serializer();
        }
        $pickled_command=null;#就是已经序列化的command
        if(is_array($command)){
            if(is_string($command[0])){
                echo ">>>>>>".$command[0];
            }
            $pickled_command[0] = $this->s->serialize($command[0]);

            if($command[1]!=null)
            $pickled_command[1]=serialize($command[1]);
            if($command[2]!=null)
            $pickled_command[2]=serialize($command[2]);
            if($command[3]!=null)
            $pickled_command[3]=serialize($command[3]);
        } else {
            $pickled_command = serialize($command);
        }

        if(strlen($pickled_command[0]) > (1 << 20)) {  # 1M
            # The broadcast will have same life cycle as created PythonRDD
            $broadcast = $sc -> broadcast($pickled_command);
            $pickled_command = serialize($broadcast);
        }

        $temp = array();
        for($i=0;$i<sizeof($sc->pickled_broadcast_vars);$i++){
            array_push($temp,$sc->pickled_broadcast_vars[$i]);
        }
        $broadcast_vars = $this->convert_list($temp,$sc);
        unset($sc->pickled_broadcast_vars);
        $env = $this->convert_map($sc->environment,$sc);
        $includes =$this-> convert_list($sc->python_includes, $sc);
        return array($pickled_command, $broadcast_vars, $env, $includes);
    }

}

class pipelined_rdd extends rdd{

    var $func;
    var $preservesPartitioning;
    var $prev_jrdd;
    var $prev_rdd;
    var $prev_jrdd_deserializer;
    var $prev_func;
    var $ctx;
    var $jrdd_val;
    var $jrdd_deserializer;
    var $bypass_serializer;
    var $partitioner;
    var $jrdd;

    function  pipelined_rdd($prev_rdd,callable $func, $preservesPartitioning=False) {
        $this->s = new Serializer();
        if(!($prev_rdd instanceof pipelined_rdd) || !$prev_rdd->is_pipelinable()) {
            echo "不是pipedrdd";
            $this->func = $func;
            $this->preservesPartitioning = $preservesPartitioning;
            $this->prev_jrdd = $prev_rdd->jrdd;
            $this->prev_jrdd_deserializer = $prev_rdd->jrdd_deserializer;
        }else {
            echo "是pipedrdd";
            $this->prev_func = $prev_rdd->func;
            $temp_prev_func =  $prev_rdd->func;
            $this->func = function ($split, $iterator) use ($func,$temp_prev_func){
                return $func($split, $temp_prev_func($split, $iterator));
            };
            $this->preservesPartitioning = $prev_rdd->preservesPartitioning && $preservesPartitioning;
            $this->prev_jrdd = $prev_rdd->prev_jrdd; # maintain the pipeline
            $this->prev_jrdd_deserializer = $prev_rdd->prev_jrdd_deserializer;
        }
        $this -> is_cached = False;
        $this -> is_checkpointed = False;
        $this -> ctx = $prev_rdd -> ctx;
        $this -> prev_rdd = $prev_rdd;
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


        $all4cmd = $tempArray[0];
        $fff = $all4cmd[0];
        #TODO $profiler $prev_jrdd_deserializer $jrdd_deserializer 没有传


        $bvars= $tempArray[1];
        $env= $tempArray[2];
        $includes = $tempArray[3];

        $python_rdd = $this->ctx->php_call_java->phpRDD(
                $this->prev_jrdd->rdd(),
                $fff,
                $env,
                $includes,
                $this->preservesPartitioning,
                $this->ctx->php_exec,
                $this->ctx->php_ver,
                $bvars,
                $this->ctx->java_accumulator);
        $this->jrdd_val = $python_rdd->getJavaRDD();
        if($profiler) {
            $this->id = $this->jrdd_val->id();
            $this->ctx->profiler_collector->add_profiler($this->id, $profiler);
            return $this->jrdd_val;
        }
        $this->jrdd = $this->jrdd_val;
    }

    function is_pipelinable(){
        return !($this->is_cached || $this->is_checkpointed);
    }
}

