<?php

$temp = __FILE__;
$spark_php_home = substr($temp,0,strrpos($temp,"/")-3);
require($spark_php_home . "src/file_output_stream.php");
require($spark_php_home . "src/file_input_stream.php");
class shuffle
{
# global stats
    static $MemoryBytesSpilled = 0;
    static $DiskBytesSpilled = 0;
}

class aggregator
{

   # Aggregator has tree functions to merge values into combiner.

   # createCombiner:  (value) -> combiner
   # mergeValue:      (combine, value) -> combiner
   # mergeCombiners:  (combiner, combiner) -> combiner

    var $createCombiner;
    var $mergeValue;
    var $mergeCombiners;


    function __construct(callable $createCombiner,callable $mergeValue,callable $mergeCombiners){
        $this->createCombiner = $createCombiner;
        $this->mergeValue = $mergeValue;
        $this->mergeCombiners = $mergeCombiners;
    }
}

class Merger
{


#    Merge shuffled data together by aggregator
    var $agg;

    function  __construct($aggregator)
    {
        $this->agg = $aggregator;
    }

    function mergeValues($iterator){
        throw new Exception("not support");

    }

    function mergeCombiners($iterator)
    {
        throw new Exception("not support");
    }

    function items(){
        throw new Exception("not support");
    }

    function get_local_dirs($sub)
    {
        $temp = __FILE__;
        $spark_php_home = substr($temp,0,strrpos($temp,"/")-3);
        $path = $spark_php_home."/tmp/";
        return $path."/php/".getmypid()."/".$sub."/";
    }

}


class ExternalMerger extends Merger{

    #External merger will dump the aggregated data into disks when
    #memory usage goes above the limit, then merge them together.
    var $MAX_TOTAL_PARTITIONS = 4096;
    var $memory_limit;
    var $serializer;
    var $localdirs;
    var $partitions;
    var $data;
    var $pdata;
    var $batch;
    var $spills;
    var $seed;

    function __construct($aggregator, $memory_limit=0.1, $serializer=null,
    $localdirs=null, $scale=1, $partitions=59, $batch=10)
    {
        parent::__construct($aggregator);
        $this->memory_limit = $memory_limit;
        $this->serializer = new utf8_serializer();#TODO
        $this->deserializer = new utf8_deserializer();
        if($localdirs==null){
            $this->localdirs = $this->get_local_dirs("shuffle");
        }else{
            $this->localdirs =$localdirs;
        }
        # number of partitions when spill data into disks
        $this->partitions = $partitions;
        # check the memory after # of items merged
        $this->batch = $batch;
        # scale is used to scale down the hash of key for recursive hash map
        $this->scale = $scale;

        # un-partitioned merged data
        $this->data = array();

        # partitioned merged data, list of dicts
        $this->pdata = array();

        # number of chunks dumped into disks
        $this->spills = 0;
        # randomize the hash of key, id(o) is the address of o (aligned by 8)
        $this->seed = intval(spl_object_hash($this))+7;
    }

    function get_spill_dir($n){# Choose one directory for spill by number n
        return $this->localdirs."/".$n."/";
    }

    function next_limit(){
        $temp = memory_get_usage()/1024/1024*1.05;
        if($temp >$this->memory_limit){
            return $temp;
        }else{
            return $this->memory_limit;
        }
    }

    function get_partition($x)
    {
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
        return hexdec(hash("md5", $x.$this->seed))%$this-> partitions;
    }

    function mergeValues($iterator)#对于key-value传进来的value合并，得到相同key的combine结果
    {
    #    """ Combine the items by creator and combiner """
        $creator = $this->agg->createCombiner;
        $comb = $this->agg->mergeValue;
        $c=0;
        $data=$this->data;
        $pdata=$this->pdata;
        $hash_func= function ($x) {


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
            return hexdec(hash("md5", $x.$this->seed))%$this-> partitions;

        };
        $batch = $this->batch;
        $limit = $this->memory_limit;
        $d = null;
        foreach($iterator as $key=>$value){#key是第几个，value是pair/array
            $key = $value[0];

            if($pdata!=null){
                $d=$pdata[$hash_func($key)];
            }elseif($d==null){
                $d=$data;
            }


            if(array_key_exists($key,$d)) {
                $d[$key] = $comb($d[$key], $value[1]);
            }else{
                $d[$key] = $creator($value[1]);
            }

            $c++;
            if($c>=$batch){
                if(memory_get_usage()/1024/1024>$limit){
                    $this->spill();
                    $limit = $this->next_limit();
                    $batch /= 2;
                    $c = 0;
                }else{
                    $batch*=1.5;
                }
            }
        }
        if(memory_get_usage()/1024/1024>$limit){
            $this->spill();
        }
        $this->data= $d;
        $this->pdata = $pdata;
    }

    function get_object_size(){
    #    How much of memory for this obj, assume that all the objects
    #    consume similar bytes of memory
        return 1;
    }

    function mergeCombiners($iterator,$limit=null){
        if($limit==null){
            $limit = $this->memory_limit;
        }
        $comb = $this->agg->mergeCombiners;
        $hash_func =  function ($x){

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
            return hexdec(hash("md5", $x.$this->seed))%$this-> partitions;

        };
        $obj_size = $this->get_object_size();
        $c = 0;
        $data = $this->data;
        $pdata = $this->pdata;
        $batch = $this->batch;
        $d=null;
        foreach($iterator as $k => $v){
            foreach($v as $key=>$value) {

                if ($pdata != null) {
                    $d = $pdata[$hash_func($key)];
                } elseif ($d == null) {
                    $d = $data;
                }

                if (array_key_exists($key, $d)) {
                    $d[$key] = $comb($d[$key], $value);
                } else {
                    $d[$key] = $value;
                }

                if ($limit == null) {
                    continue;
                }

                $c += $this->get_object_size();
                if ($c > $batch) {
                    if (memory_get_usage() / 1024 / 1024 > $limit) {#TODO
                        $this->spill();
                        $limit = $this->next_limit();
                        $batch /= 2;
                        $c = 0;
                    } else {
                        $batch *= 1.5;
                    }
                }
            }
        }
        if($limit != null && memory_get_usage()/1024/1024 >= $limit){
            $this->spill();
        }


        $this->data= $d;
        $this->pdata = $pdata;
    }

    function spill(){

        if($this->data==null){
           return;
        }

        #$path = $this->get_spill_dir($this->spills);#TODO
        $path="/home/".get_current_user()."/php_temp/";
        if(!file_exists($path)) {
            mkdir($path,0777,True);
        }
        $used_memory = memory_get_usage()/1024/1024;
        if($this->pdata==null||sizeof($this->pdata)==0)
        {
            # The data has not been partitioned, it will iterator the
            # dataset once, write them into different files, has no
            # additional memory. It only called when the memory goes
            # above limit at the first time.

            # open all the files for writing
            $file_streams = array();
            for($i=0;$i<$this->partitions;$i++){
                $f = fopen($path."/".$i,"wb");
                array_push($file_streams,new file_output_stream($f));
            }

            foreach($this -> data as $key=>$value) { #TODO 注意
                $h = $this->get_partition($key);
                $this->serializer->dump_stream4file(array(array($key, $value)), $file_streams[$h]);
            }

            $c = 0;
            foreach($file_streams as $fs){
                shuffle::$DiskBytesSpilled += filesize($path."/".$c);

                fclose($fs->get_file());
            }

            unset($this->data);
            if($this->pdata==null)
            {
                $this->pdata=array();
            }
            for($i=0;$i<$this->partitions;$i++){
                array_push($this->pdata,array());
            }

        }else{
            for($i=0;$i<$this->partitions;$i++){
                $p = $path."/".$i;
                $f = fopen($p,"wb");
                $this->serializer->dump_stream4file($this->pdata[$i]->items(), new file_output_stream($f));
                unset($this->pdata[$i]);
                shuffle::$DiskBytesSpilled += filesize($p);
            }
        }
        $this->spills += 1;
        #TODO gc.collect()  # release the memory as much as possible
        shuffle::$MemoryBytesSpilled += max($used_memory - memory_get_usage()/1024/1024, 0) << 20;
    }


    function items(){
    #    """ Return all merged items as iterator """
        if($this->pdata==null && $this->spills==null) {#如果硬盘没数据
            return $this -> data;
        }
        return $this->external_items();
    }

    function external_items(){
        #""" Return all partitioned items as iterator """
        if($this->data==null){
            throw new Exception("can not reach here");
        }
        $flag = True;
        foreach($this->pdata as $value){
            if($value==null||sizeof($value)==0){
                $flag=False;
            }
        }
        if($flag) {
            $this->spill();
        }
        # disable partitioning and spilling when merge combiners from disk
        $this->pdata = array();

        try {
            for($i=0; $i<$this->partitions ;$i++){
                $result = array();
                for($v=0;$v<$this->merged_items($i);$v++) {
                    array_push($result,$v);
                }

                unset($this->$data);

                # remove the merged partition
                for($j=0;$j<$this->spills;$j++){
                    $path = $this->get_spill_dir($j);
                    fclose($path.$i);
                }
                return $result;
            }
        }finally {
            $this->cleanup();
        }

    }

    function merged_items($index){
        $this->data = array();
        $limit = $this->next_limit();
        for($i = 0; $i < $this->spills ;$i++) {
            #$path = $this->get_spill_dir($i);#TODO
            $path="/home/".get_current_user()."/php_temp/";
            $p = $path . "/" . $index;
            $f = fopen($p, "rb");

            $iter = $this->deserializer->load_stream4file(new file_input_stream($f));

            $this->mergeCombiners($iter, 0);

            if ($this->scale * $this->partitions < $this->MAX_TOTAL_PARTITIONS
                && $i < $this->spills - 1
                && memory_get_usage() / 1024 / 1024 > $limit
            ) {
                unset($this->data);  # will read from disk again
                #     gc.collect()  # release the memory as much as possible
                return $this->recursive_merged_items($index);
            }
        }
        return $this->data->items();
    }

    function recursive_merged_items($index){
        $sub_dirs = array();
        for($i=0;$i<$this->localdirs;$i++){
            array_push($sub_dirs,$this->localdirs[$i]."/parts/".$index);
        }
        $m = ExternalMerger($this->agg, $this->memory_limit, $this->serializer, $sub_dirs,
            $this->scale * $this->partitions, $this->partitions, $this->batch);

        $m->pdata=array();
        for($i=0;$i<$this->partitions;$i++){
            array_push($m->pdata,array());
        }
        $limit = $this->next_limit();

        for($i=0;$i<$this->spills;$i++){
           # $path = $this->get_spill_dir($i);#TODO
            $path="/home/".get_current_user()."/php_temp/";
            $p = $path."/".$index;
            $f= fopen($p,"rb");
            $m->mergeCombiners($this->serializer->load_stream4file($f),0);
            if(memory_get_usage() / 1024 / 1024 >$limit){
                $m->spill();
                $limit= $this->next_limit();
            }
        }
        return $this->external_items();
    }

    function cleanup()
    {
       # """ Clean up all the files in disks """
        foreach($this->localdirs as $d) {
            unlink($d);
        }
    }
}

class ExternalGroupBy extends ExternalMerger
{
  static  $SORT_KEY_LIMIT = 1000;



}