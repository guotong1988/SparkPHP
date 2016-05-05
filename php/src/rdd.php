<?php


$temp = __FILE__;
$spark_php_home = substr($temp,0,strrpos($temp,"/")-3);
require($spark_php_home . "src/sock_input_stream.php");
require($spark_php_home . "src/sock_output_stream.php");
require($spark_php_home . "src/shuffle.php");
require($spark_php_home . "src/rddsampler.php");
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
                return array($count);
            }

        )->sum();

    }


    function first()
    {
        $rs = $this->take(1);
        if($rs!=null){
            return $rs[0];
        }
        throw new Exception("no data");
    }

    function filter($f){
        $func = function ($iterator)use ($f){
            if($iterator instanceof Generator){
                $temp = array();
                foreach($iterator as $ele){
                    array_push($temp,$ele);
                }
                return array_filter($temp,$f);
            }
            return array_filter($iterator,$f);
        };
        return $this->mapPartitions($func, True);
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
        $is_list = function ($arr){
            return is_array($arr) && ($arr == array() || array_keys($arr) === range(0,count($arr)-1) );
        };

        return $this->mapPartitionsWithIndex(

            function ($any, $iterator) use ($f,$is_list) {#TODO 注意$iterator的每个为kv的情况
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
        $ADD = function ($x0,$x1){
            return $x0+$x1;
        };
        return $this->mapPartitions(
            function ($iterator) {
                if($iterator instanceof Generator){
                    $sum = 0;
                    foreach($iterator as $e){
                        $sum = $sum + $e;
                    }
                    return array($sum);
                }elseif(is_integer($iterator)){
                    return $iterator;
                }else{
                    return array(array_sum($iterator));
                }
            }
        )->fold(0, $ADD);
    }

    function fold($zeroValue, $op)
    {
        $temp = $this->mapPartitions(

            function ($iterator) use ($zeroValue, $op) {
                $acc = $zeroValue;
                foreach ($iterator as $element) {
                        $acc = $op($element,$acc);
                }
                yield $acc;
            }
        )->collect();

        if($temp instanceof Generator){
            $temp2 = array();
            foreach($temp as $e){
                array_push($temp2, $e);
            }
            return array_reduce($temp2,

                function ($v0, $v1) {
                    return $v0 + $v1;
                }

                , $zeroValue);
        }else {
            return array_reduce($temp,

                function ($v0, $v1) {
                    return $v0 + $v1;
                }

                , $zeroValue);
        }
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
        #socket_close($sock);#改成yield之后不能关了
        return $item_array;
    }

    function memory_limit(){
        return 128;#TODO
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

    function saveAsTextFile($path){
        $temp = $this->jrdd->map($this->ctx->php_call_java->BytesToString());
        $temp ->saveAsTextFile($path);
    }



    function combineByKey(callable $createCombinerFunc, callable $mergeValueFunc, callable $mergeCombinersFunc,
        $numPartitions=null, callable $partitionFunc=null)
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


        if($numPartitions==null) {
            $numPartitions = $this->defaultReducePartitions();
        }

        $serializer = $this->ctx->serializer;

        $memory =  $this->memory_limit();


        $locally_combined = $this->mapPartitions(

            function ($iterator) use ($memory,$serializer,$createCombinerFunc, $mergeValueFunc, $mergeCombinersFunc){
                $agg = new aggregator($createCombinerFunc, $mergeValueFunc, $mergeCombinersFunc);
                $merger = new ExternalMerger($agg, $memory * 0.9, $serializer);
                $merger -> mergeValues($iterator);
                $re = $merger->items();
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

                    if ($c % 1000 == 0 && memory_get_usage()/1024/1024 > $limit || $c > $batch) {
                        $n = sizeof($buckets);
                        $size = 0;

                        foreach($buckets as $split => $pair) { #value是一个array
                            yield pack('J', $split);
                            $temp2 = array();
                            foreach($pair as $k =>$v){
                                $temp2[$k]=$v;
                            }
                            yield serialize($temp2);#给PairwiseRDD使用
                        }
                        unset($buckets);
                        $buckets = array();
                        $avg = intval($size / $n) >> 20;
                        # let 1M < avg < 10M
                        if($avg < 1){
                            $batch *= 1.5;
                        } elseif($avg > 10){
                            $batch = max(intval($batch / 1.5), 1);
                        }
                        $c = 0;
                    }
                }

                foreach($buckets as $split => $pair) {
                    yield pack('J', $split);
                    $temp = array();
                    foreach($pair as $k =>$v){
                        $temp[$k]=$v;
                    }
                    yield serialize($temp);#给PairwiseRDD使用
                }
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

                if($iterator instanceof Generator) {
                    $temp2 = array();
                    foreach ($iterator as $e) {
                        array_push($temp2, $e);
                    }
                    return array_reduce($temp2,$f);
                }

                return array_reduce($iterator,$f);
            }

        )->collect();
        if($temp!=null){
            if($temp instanceof Generator) {
                $temp2 = array();
                foreach ($temp as $e) {
                    array_push($temp2, $e);
                }
                return array_reduce($temp2,$f);
            }
            else {
                return array(array_reduce($temp, $f));
            }
        } else {
            throw new Exception("Can not reduce() empty RDD");
        }
    }

    function sortByKey($ascending=True, $numPartitions=null, callable $keyFunc=null){
        if($numPartitions==null){
            $numPartitions=$this->defaultReducePartitions();
        }
        $memory = $this->memory_limit();
        $serializer = $this->deserializer;

        $func = function ($kv) use ($keyFunc) {
            return $keyFunc($kv[0]);
        };

        $sortPartition = function  ($iterator) use ($func,$memory,$serializer,$ascending) {
            $temp = new ExternalSorter($memory * 0.9, $serializer);
            return $temp -> sorted($iterator,$func,!$ascending);
        };

        #TODO if numPartitions == 1:

        $rddSize = $this->count();
        if($rddSize==0){
            return $this; # empty RDD
        }

        $maxSampleSize = $numPartitions * 20.0;  # constant from Spark's RangePartitioner
        $fraction = min($maxSampleSize / max($rddSize, 1), 1.0);
        $samples = $this->sample(False, $fraction, 1)->map(
                function ($kv){
                    return $kv[0];
                }
            )->collect();

    }

    function take($num){
        $items = array();
        $totalParts = $this->getNumPartitions();

        $partsScanned=0;
        while(sizeof($items)<$num and $partsScanned<$totalParts){
            $numPartsToTry = 1;
            if($partsScanned>0){
                if(sizeof($items)==0){
                    $numPartsToTry = $partsScanned * 4;
                }else{
                    $numPartsToTry = intval(1.5*$num*$partsScanned/sizeof($items))-$partsScanned;
                    $numPartsToTry = min(max($numPartsToTry, 1), $partsScanned * 4);
                }
            }
            $left = $num - sizeof($items);

            $takeUpToNumLeft = function ($iterator)use($left){
                $taken = 0;
                foreach($iterator as $e) {
                    $taken += 1;
                    if($taken>$left){
                        break;
                    }
                    yield $e;
                }
            };
            $p = range($partsScanned, min($partsScanned + $numPartsToTry, $totalParts)-1);

            $p=$this->convert_list($p,$this->ctx);

            $res = $this->ctx->runJob($this, $takeUpToNumLeft, $p);
            foreach($res as $e) {
                array_push($items,$e);
            }
            $partsScanned += $numPartsToTry;

        }
        $result = array();
        for($i=0;$i<$num;$i++){
            array_push($result,$items[$i]);
        }
        return $result;
    }



/**
 *  @param withReplacement: can elements be sampled multiple times (replaced when sampled out)
 *  @param fraction: 取百分之多少的数据，在0-1之间
 *  @param seed: 关于random的方法
 */
    function sample($withReplacement, $fraction, $seed=null){
        $temp_func = function ($split,$iterator) use ($withReplacement,$fraction,$seed){
            $rdd_sampler = new rdd_sampler($withReplacement, $fraction, $seed);
            return $rdd_sampler->func($split,$iterator);
        };
        return $this->mapPartitionsWithIndex($temp_func, True);
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
            $jmap->put($key,$value);
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


        if($this->jrdd!=null){

        }else {
            if ($this->bypass_serializer) {
#            $this->jrdd_deserializer = new NoOpSerializer();
            }
            $profiler = null;
            if ($this->ctx->profiler_collector) {
                $profiler = $this->ctx->profiler_collector->new_profiler($this->ctx);
            } else {
                $profiler = null;
            }
            $command = array();
            $command[0] = $this->func;
            $command[1] = $profiler;
            $command[2] = $this->prev_jrdd_deserializer;
            $command[3] = $this->jrdd_deserializer;


            $tempArray = $this->prepare_for_python_RDD($this->ctx, $command, $this);


            $all4cmd = $tempArray[0];
            $fff = $all4cmd[0];
            #TODO $profiler $prev_jrdd_deserializer $jrdd_deserializer 没有传


            $bvars = $tempArray[1];
            $env = $tempArray[2];
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
            if ($profiler) {
                $this->id = $this->jrdd_val->id();
                $this->ctx->profiler_collector->add_profiler($this->id, $profiler);
                return $this->jrdd_val;
            }
            $this->jrdd = $this->jrdd_val;
        }
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

