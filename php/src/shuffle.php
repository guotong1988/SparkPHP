<?php

class shuffle
{
# global stats
    static $MemoryBytesSpilled = 0;
    static $DiskBytesSpilled = 0;
}

class Aggregator
{

   # Aggregator has tree functions to merge values into combiner.

   # createCombiner:  (value) -> combiner
   # mergeValue:      (combine, value) -> combiner
   # mergeCombiners:  (combiner, combiner) -> combiner

    var $createCombiner;
    var $mergeValue;
    var $mergeCombiners;


    function __construct($createCombiner, $mergeValue, $mergeCombiners){
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
        $path = "/tmp"; #TODO
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
    var $spill;
    var $seed;

    function __construct($aggregator, $memory_limit=512, $serializer=null,
    $localdirs=null, $scale=1, $partitions=59, $batch=1000)
    {
        parent::__construct($aggregator);
        $this->memory_limit = $memory_limit;
        $this->serializer = $serializer;#TODO
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
        $this->spill = 0;
        # randomize the hash of key, id(o) is the address of o (aligned by 8)
        $this->seed = intval(spl_object_hash($this))+7;
    }

    function get_spill_dir($n){# Choose one directory for spill by number n
        return $this->localdirs.$n."/";
    }

    function next_limit(){
        $temp = memory_get_usage()/1024/1024*1.05;
        if($temp >$this->memory_limit){
            return $temp;
        }else{
            return $this->memory_limit;
        }
    }

    function get_partition($key)
    {
        return hash("md5",$key.$this->seed) % $this-> partitions;
    }

    function mergeValues($iterator)
    {
    #    """ Combine the items by creator and combiner """
        $creator = $this->agg->createCombiner;
        $comb = $this->agg->mergeValue;
        $c=0;
        $data=$this->data;
        $pdata=$this->pdata;
        $hash_func=$this->get_partition;
        $batch = $this->batch;
        $limit = $this->memory_limit;

        foreach($iterator as $key=>$value){
            $d = null;
            if($pdata!=null){
                $d=$pdata[$hash_func($key)];
            }else{
                $d=$data;
            }
            if(in_array($key,$d)) {
                $d[$key] = $comb($d[$key], $value);
            }else{
                $d[$key] = $creator($value);
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
        $hash_func = $this->get_partition;
        $obj_size = $this->get_object_size();
        $c = 0;
        $data = $this->data;
        $pdata = $this->data;
        $batch = $this->batch;

        foreach($iterator as $key => $value){
            $d=null;
            if($pdata!=null){
                $d = $pdata[$hash_func($key)];
            }else {
                $d = $data;
            }

            if(in_array($key,$d)) {
                $d[$key] = $comb($d[$key], $value);
            }else{
                $d[$key] = $value;
            }

            if($limit==null){
                continue;
            }

            $c+=$this->get_object_size();
            if($c>$batch){
                if(memory_get_usage()/1024/1024>$limit){
                    $this->spill();
                    $limit = $this->next_limit();
                    $batch /= 2;
                    $c = 0;
                }else{
                    $batch *= 1.5;
                }
            }
        }
        if($limit != null && memory_get_usage()/1024/1024 >= $limit){
            $this->spill();
        }
    }

    function spill(){
        $path = $this->get_spill_dir($this->spills);
        if(file_exists($path)) {
                mkdir($path);
        }
        $used_memory = memory_get_usage()/1024/1024;
        if($this->pdata==null)
        {
            # The data has not been partitioned, it will iterator the
            # dataset once, write them into different files, has no
            # additional memory. It only called when the memory goes
            # above limit at the first time.

            # open all the files for writing
            $files = array();
            for($i=0;$i<$this->partitions;$i++){
                $f = fopen($path.$i,"wb");
                array_push($files,$f);
            }

            foreach($this -> data -> items() as $key=>$value) { #TODO 注意
                $h = $this->get_partition($key);

               #TODO self . serializer . dump_stream([(k, v)], streams[h])
            }
        }else{
            for($i=0;$i<$this->partitions;$i++){
                $f = fopen($path.$i,"wb");
                #TODO
            }
        }
        $this->spills += 1;
        #TODO gc.collect()  # release the memory as much as possible
        shuffle::$MemoryBytesSpilled += max($used_memory - memory_get_usage()/1024/1024, 0) << 20;
    }


    function items(){
    #    """ Return all merged items as iterator """
        if($this->pdata==null && $this->spill==null) {
            return $this -> data -> items();
        }
        return $this->external_items();
    }

    function external_items(){
        #""" Return all partitioned items as iterator """



    }



}