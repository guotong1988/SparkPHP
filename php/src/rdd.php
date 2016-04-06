<?php


$temp = __FILE__;
$spark_php_home = substr($temp,0,strrpos($temp,"/")-3);
require($spark_php_home . "src/sock_input_stream.php");
require($spark_php_home . "src/shuffle.php");
require 'vendor/autoload.php';
use SuperClosure\Serializer;


class rdd
{

    var $jrdd;
    var $is_cached;
    var $is_checkpointed;
    var $ctx;
    var $deserializer;
    var $id;
    var $partitioner;
    var $s;

    function rdd($jrdd, $ctx, $deserializer)
    {

        $this->jrdd = $jrdd;
        $this->is_cached = False;
        $this->is_checkpointed = False;
        $this->ctx = $ctx;
        $this->deserializer = $deserializer;
        $this->id = $jrdd->setName("!!!");
        $this->partitioner = null;
        $this->s = new Serializer();
    }

    function id()
    {
        return $this->id;
    }

    function count()
    {

        return $this->mapPartitions(

            function ($iterator) {
                $count = 0;
                foreach ($iterator as $element) {
                    $count++;
                }
                return $count;
            }

        )->sum();

    }

    function flatMap(callable $f, $preservesPartitioning = False)
    {

        return $this->mapPartitionsWithIndex(

            function ($split, $iterator) use ($f) {

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
            },
            $preservesPartitioning);
    }

    function map(callable $f, $preservesPartitioning = False)
    {

        return $this->mapPartitionsWithIndex(

            function ($any, $iterator) use ($f) {
                return array_map($f, $iterator);
            }

            , $preservesPartitioning);
    }


    function mapPartitions(callable $f, $preservesPartitioning = False)
    {
        return $this->mapPartitionsWithIndex(

            function ($split, $iterator) use ($f) {
                return $f($iterator);
            }

            , $preservesPartitioning);
    }

    function mapPartitionsWithIndex(callable $f, $preservesPartitioning = False)
    {
        return new pipelined_rdd($this, $f, $preservesPartitioning);
    }

#        >>> sc.parallelize([1.0, 2.0, 3.0]).sum()
#        输出6.0
    function sum()
    {
        $ADD = 1;
        return $this->mapPartitions(
            function ($iterator) {
                return array(array_sum($iterator));
            }
        )->fold(0, $ADD);
    }

    function fold($zeroValue, $op)
    {
        $temp = $this->mapPartitions(

            function ($iterator) use ($zeroValue, $op) {
                $acc = $zeroValue;
                foreach ($iterator as $element) {
                    $ADD = 1;
                    if($op==$ADD) {
                        $acc = $element + $acc;
                    }
                }
                $temp = array();
                array_push($temp, $acc);
                return $temp;
            }

        )->collect();

        return array_reduce($temp,

            function ($v0, $v1) {
                return $v0 + $v1;
            }

            , $zeroValue);
    }


    function collect()
    {
        #TODO    with SCCallSiteSync(self . context) as css:
        $port = $this->ctx->php_call_java->PhpRDD->collectAndServe($this->jrdd->rdd());
        return $this->load_from_socket($port, $this->deserializer);
    }


    function load_from_socket($port, $deserializer)
    {
        $sock = socket_create(AF_INET, SOCK_STREAM, SOL_TCP);
        if ($sock == false) {
            echo "socket_create()失败:" . socket_strerror(socket_last_error($sock)) . "\n";
        } else {
            echo "socket_create()成功\n";
        }

        $port = $port->intValue() . "";#不然不行，坑啊

        $result = socket_connect($sock, '127.0.0.1', intval($port));
        if ($result == false) {
            echo "socket_connect()失败:" . socket_strerror(socket_last_error($sock)) . "\n";
        } else {
            echo "socket_connect()成功\n";
        }
        $stream = new sock_input_stream($sock);
        if ($deserializer == null) {
            $deserializer = new utf8_deserializer();
        }
        $item_array = $deserializer->load_stream($stream);
        socket_close($sock);
        return $item_array;
    }

    function memory_limit(){
        return 512;#TODO
    }

    function reduceByKey(callable $func, $numPartitions=null,callable $partitionFunc=null)
    {
        if($partitionFunc==null){
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
        }

        return $this->combineByKey(
            function ($x){
                return $x;
            }

            ,$func, $func, $numPartitions,$partitionFunc);
    }

    function combineByKey(callable $createCombinerFunc, callable $mergeValueFunc, callable $mergeCombinersFunc,
        $numPartitions=null, callable $partitionFunc=null)
    {
        if($numPartitions==null) {
            $numPartitions = $this->defaultReducePartitions();
        }

        $serializer = $this->ctx->serializer;

        $memory =  $this->memory_limit();


        $locally_combined = $this->mapPartitions(

            function ($iterator) use ($memory,$serializer,$createCombinerFunc, $mergeValueFunc, $mergeCombinersFunc){

                file_put_contents("/home/gt/php_worker36.txt", sizeof($iterator)."\n", FILE_APPEND);
                $agg = new aggregator($createCombinerFunc, $mergeValueFunc, $mergeCombinersFunc);
                $merger = new ExternalMerger($agg, $memory * 0.9, $serializer);
                $merger -> mergeValues($iterator);
                $re = $merger->items();
                file_put_contents("/home/gt/php_worker37.txt", sizeof($re)."\n", FILE_APPEND);
                return $re;
            },

            True);
        $shuffled = $locally_combined->partitionBy($numPartitions, $partitionFunc);
        return $shuffled->mapPartitions(

            function ($iterator)use ($memory,$serializer,$createCombinerFunc, $mergeValueFunc, $mergeCombinersFunc){
                $agg = new aggregator($createCombinerFunc, $mergeValueFunc, $mergeCombinersFunc);
                $merger = new ExternalMerger($agg, $memory, $serializer);
                $merger -> mergeCombiners($iterator);
                return $merger->items();
            }

            , True);
    }


    function defaultReducePartitions()
    {
        if($this->ctx->conf->contains("spark.default.parallelism")){
            return $this->ctx->defaultParallelism;
        } else {
            return $this->getNumPartitions();
        }
    }

    function partitionBy($numPartitions,callable $partitionFunc=null)
    {
        if ($partitionFunc == null) {
            $partitionFunc =  function ($x) {
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
        }

        if ($numPartitions == null) {
            $numPartitions = $this->defaultReducePartitions();
        }

        $partitioner = new Partitioner($numPartitions, $partitionFunc);
        if($this->partitioner != null && serialize($this->partitioner) == serialize($partitioner)) {
            return $this;
        }
        $outputSerializer = $this->ctx->unbatched_serializer;#TODO

        $limit=256;

        $keyed = $this->mapPartitionsWithIndex(

            function($split,$iterator)use($numPartitions,$partitionFunc,$limit,$outputSerializer){
                $buckets = array();
                $c=0;
                $batch=min(10*$numPartitions,1000);



                $result = array();
                foreach($iterator as $key=>$value){#wordcount为例，这是word=>count
                    $temp = $partitionFunc($key) % $numPartitions;#相同的key汇集到一起

                    if($buckets[$temp]==null) {
                        $buckets[$temp] = array();
                    }
                    $buckets[$temp][$key]=$value;
                    $c++;

                   /* if ($c % 1000 == 0 && memory_get_usage()/1024/1024 > $limit || $c > $batch) {
                        $n = sizeof($buckets);
                        $size = 0;

                        foreach($buckets as $split => $pair) { #value是一个array
                            array_push($result,pack('J', $split));
                            $temp2 = array();
                            foreach($pair as $k =>$v){
                                $temp2[$k]=$v;
                            }
                            array_push($result,serialize($temp2));
                        }

                        $avg = intval($size / $n) >> 20;
                        # let 1M < avg < 10M
                        if($avg < 1){
                            $batch *= 1.5;
                        } elseif($avg > 10){
                            $batch = max(intval($batch / 1.5), 1);
                        }
                        $c = 0;

                    }*/
                }
                foreach($buckets as $split => $pair) {
                    array_push($result,pack('J', $split));
                    $temp = array();
                    foreach($pair as $k =>$v){
                        $temp[$k]=$v;
                    }
                    array_push($result,serialize($temp));
                }
                return $result;#给PairwiseRDD使用
            }


        , True);
        $keyed->bypass_serializer = True;

        
      #TODO  with SCCallSiteSync(self.context) as css:
        $pairRDD = $this->ctx->php_call_java->pair_wise_rdd(
                    $keyed->jrdd->rdd())->asJavaPairRDD();
        $jpartitioner = $this->ctx->php_call_java->php_partitioner($numPartitions,
            intval(spl_object_hash($partitionFunc)));#TODO long
        $jrdd = $this->ctx->php_call_java->PhpRDD->valueOfPair($pairRDD->partitionBy($jpartitioner));
        $rdd = new rdd($jrdd, $this->ctx,$outputSerializer);#TODO $outputSerializer
        $rdd->partitioner = $partitioner;
        return $rdd;

    }


    function getNumPartitions()
    {
        return $this-> jrdd -> partitions() -> size();
    }

    function glom()
    {
        /*
        Return an RDD created by coalescing all elements within each partition
        into a list.

        >>> rdd = sc.parallelize([1, 2, 3, 4], 2)
        >>> sorted(rdd.glom().collect())
        [[1, 2], [3, 4]]
        */

        $func = function ($iterator){
            $result = array();
            foreach($iterator as $key => $value){
                array_push($result,$value);
            }
            return $result;
        };
        return $this->mapPartitions($func);
    }

    function reduce($f){
        $temp = $this->mapPartitions(

            function ($iterator) use ($f){
                return array_reduce($iterator,$f);
            }

        )->collect();
        if($temp!=null){
            return array(array_reduce($temp,$f));
        } else {
            throw new Exception("Can not reduce() empty RDD");
        }
    }

    function groupByKey($numPartitions=null, $partitionFunc=null){
        if($numPartitions==null){
            $numPartitions=$this->defaultReducePartitions();
        }
        if($partitionFunc==null){
            $partitionFunc= function ($x) {
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
        }

        $createCombiner=function($x) {
            return $x;
        };

        $mergeValue=function($xs, $x) {
            array_push($xs,$x);
            return $xs;
        };

        $mergeCombiners=function($a, $b) {
            if(is_string($a)){
                return array($a,$b);
            }else{
                if(is_array($b)){
                    foreach($b as $ele) {
                        array_push($a, $ele);
                    }
                }else {
                    array_push($a, $b);
                }
                return $a;
            }
        };
        $memory = $this->memory_limit();
        $serializer = $this->deserializer;

        $combine= function ($iterator)use ($createCombiner, $mergeValue, $mergeCombiners,$memory,$serializer) {
            $agg =new  aggregator($createCombiner, $mergeValue, $mergeCombiners);
            $merger =new ExternalMerger($agg, $memory * 0.9, $serializer);
            $merger->mergeValues($iterator);
            return $merger->items();
        };

        $locally_combined = $this->mapPartitions($combine, True);
        $shuffled = $locally_combined->partitionBy($numPartitions, $partitionFunc);

        $groupByKey=function($it)use ($createCombiner, $mergeValue, $mergeCombiners,$memory,$serializer)  {
            $agg =new  aggregator($createCombiner, $mergeValue, $mergeCombiners);
            $merger = new ExternalGroupBy($agg, $memory, $serializer);
            $merger->mergeCombiners($it);
            return $merger->items();
        };

        return $shuffled->mapPartitions($groupByKey, True)->mapValues(

            function ($x){
                return $x;
            }
        );
    }

    function keyBy($f){
        return $this->map(
            function ($everyOne) use ($f){
                return array($f($everyOne), $everyOne);
            }
        );
    }


    function mapValues(callable $f){

        $map_values_func = function($input) use ($f){#$input不是iterator
            if(is_array($input)){
                $re = array();
                $index = 0;
                foreach($input as $k=>$v){
                    if($index==0){
                        array_push($re,$v);
                    }
                    if($index==1) {
                        array_push($re,$f($v));
                    }
                    $index++;
                    if($index==2){
                        $index=0;
                    }
                }
                return $re;
            }else{
                return $f($input);
            }
        };

        return $this->map($map_values_func, True);
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
            $this->func = $func;
            $this->preservesPartitioning = $preservesPartitioning;
            $this->prev_jrdd = $prev_rdd->jrdd;
            $this->prev_jrdd_deserializer = $prev_rdd->jrdd_deserializer;
        }else {
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

class Partitioner{

    var $numPartitions;
    var $partitionFunc;

    function __construct($numPartitions,$partitionFunc){
        $this->numPartitions = $numPartitions;
        $this->partitionFunc = $partitionFunc;
    }

    #TODO   def __call__(self, k):
}

