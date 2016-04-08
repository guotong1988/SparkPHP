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

    function __construct($aggregator, $memory_limit=1, $serializer=null,
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
        $this->partitions = $partitions;#和pdata的容器数相同
        # partitioned merged data, list of dicts
        $this->pdata = array();

        $this->batch = $batch;
        # scale is used to scale down the hash of key for recursive hash map
        $this->scale = $scale;

        # un-partitioned merged data
        $this->data = array();

        # spill的次数
        $this->spills = 0;
        # randomize the hash of key, id(o) is the address of o (aligned by 8)
        $this->seed = intval(spl_object_hash($this))+7;
    }

    function get_spill_dir($n){# Choose one directory for spill by number n
        return $this->localdirs."/".$n."/";
    }

    function next_limit(){
        $temp = memory_get_usage()/1024/1024*1.05;
        if($temp > $this->memory_limit){
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
            return $h%$this-> partitions;
        }
        return hexdec(hash("md5", $x))%$this-> partitions;
    }

    function mergeValues($iterator)#对于key-value传进来的value合并，得到相同key的combine结果
    {
    #    """ Combine the items by creator and combiner """
        $creator = $this->agg->createCombiner;
        $comb = $this->agg->mergeValue;
        $c=0;
        $data=$this->data;
        $pdata=$this->pdata;

        $hasSpilled=False;

        $hash_func= function ($x) {


            if ($x == null) {
                return 0;
            }
            if (is_array($x)) {
                $h=0;
                foreach($x as $ele){
                    $h ^= hexdec(hash("md5", $ele));
                }
                return $h%$this-> partitions;
            }
            return hexdec(hash("md5", $x))%$this-> partitions;

        };
        $batch = $this->batch;
        $limit = $this->memory_limit;
        $d = null;
        foreach($iterator as $key=>$value){#key是第几个，value是pair/array，这个pair是word和count
            $key = $value[0];
            if (sizeof($pdata)>0) {
                if (array_key_exists($key,  $pdata[$hash_func($key)])) {
                    $pdata[$hash_func($key)][$key] = $comb($d[$key], $value[1]);
                } else {
                    $pdata[$hash_func($key)][$key] = $value[1];
                }
            } else {
                if ($d == null) {
                    $d = array();
                }
                if (array_key_exists($key, $d)) {
                    $d[$key] = $comb($d[$key], $value[1]);
                } else {
                    $d[$key] = $value[1];
                }
            }

            $c++;
            if($c>=$batch){
                if(memory_get_usage()/1024/1024>$limit){
                    $this->data= $d;
                    $this->pdata = $pdata;
                    $this->spill();
                    $hasSpilled = True;
                    unset($d);
                    unset($pdata);
                    $d=array();
                    $pdata=array();
                    $limit = $this->next_limit();
                    $batch /= 2;
                    $c = 0;
                }else{
                    $batch*=1.5;
                }
            }
        }
        $this->data= $d;
        $this->pdata = $pdata;
        if(memory_get_usage()/1024/1024>$limit || $hasSpilled==True){
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
        $hash_func =  function ($x){

            if ($x == null) {
                return 0;
            }
            if (is_array($x)) {
                $h=0;
                foreach($x as $ele){
                    $h ^= hexdec(hash("md5", $ele));
                }
                return $h%$this-> partitions;
            }
            return hexdec(hash("md5", $x))%$this-> partitions;

        };

        $hasSpilled=False;
        $obj_size = $this->get_object_size();
        $c = 0;
        $pdata = $this->pdata;
        $batch = $this->batch;
        $d=null;
        foreach($iterator as $k => $v){

            if(is_array($v)){
                foreach($v as $key=>$value) {
                    if (sizeof($pdata)>0) {
                        if (array_key_exists($key,  $pdata[$hash_func($key)])) {
                            $pdata[$hash_func($key)][$key] = $comb($d[$key], $value);
                        } else {
                            $pdata[$hash_func($key)][$key] = $value;
                        }
                    } else {
                        if ($d == null) {
                            $d = array();
                        }
                        if (array_key_exists($key, $d)) {
                            $d[$key] = $comb($d[$key], $value);
                        } else {
                            $d[$key] = $value;
                        }
                    }
                    if ($limit == null||$limit==-1) {
                        continue;
                    }

                    $c += $this->get_object_size();
                    if ($c > $batch) {
                        if ($limit!=-1 && memory_get_usage() / 1024 / 1024 > $limit) {
                            $this->data= $d;
                            $this->pdata = $pdata;
                            $this->spill();
                            unset($d);
                            unset($pdata);
                            $d=array();
                            $pdata=array();

                            $hasSpilled=True;
                            $limit = $this->next_limit();
                            $batch /= 2;
                            $c = 0;
                        } else {
                            $batch *= 1.5;
                        }
                    }
                }
            }else{
                if (sizeof($pdata)>0) {
                    if (array_key_exists($k,  $pdata[$hash_func($k)])) {
                        $pdata[$hash_func($k)][$k] = $comb($d[$k], $v);
                    } else {
                        $pdata[$hash_func($k)][$k] = $v;
                    }
                } else {
                    if ($d == null) {
                        $d = array();
                    }
                    if (array_key_exists($k, $d)) {
                        $d[$k] = $comb($d[$k], $v);
                    } else {
                        $d[$k] = $v;
                    }
                }
                if ($limit == -1 || $limit==null) {
                    continue;
                }

                $c += $this->get_object_size();
                if ($c > $batch) {
                    if ($limit!=-1 && memory_get_usage() / 1024 / 1024 > $limit) {
                        $this->data= $d;
                        $this->pdata = $pdata;
                        $this->spill();
                        unset($d);
                        unset($pdata);
                        $d=array();
                        $pdata=array();
                        $hasSpilled=True;
                        $limit = $this->next_limit();
                        $batch /= 2;
                        $c = 0;
                    } else {
                        $batch *= 1.5;
                    }
                }
            }
        }

        $this->data= $d;
        $this->pdata = $pdata;
        if($limit!=-1) {
            if(memory_get_usage()/1024/1024 >= $limit||$hasSpilled==True) {
                $this->spill();
            }
        }
    }

    function spill(){

        #$path = $this->get_spill_dir($this->spills);#TODO
        $path="/home/".get_current_user()."/php_temp/";
        if(!file_exists($path)) {
            mkdir($path,0777,True);
        }else{
            $this->cleanup();
        }

        $used_memory = memory_get_usage()/1024/1024;
        if(sizeof($this->pdata)==0)
        {
            if($this->data==null||sizeof($this->data)==0){
                return;
            }

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

            foreach($this -> data as $key=>$value) {
                $h = $this->get_partition($key);
                $temp = array();
                $temp[$key] = $value;
                $this->serializer->dump_stream4file($temp, $file_streams[$h]);
            }

            $c = 0;
            foreach($file_streams as $fs){
                shuffle::$DiskBytesSpilled += filesize($path."/".$c);
                fclose($fs->get_file());
                $c++;
            }
            unset($this->data);
            $this->data=array();
            if($this->pdata==null)
            {
                $this->pdata=array();
            }
            for($i=0;$i<$this->partitions;$i++){
                array_push($this->pdata,array());
            }

        }else{

            if($this->pdata==null||sizeof($this->pdata)==0){
                return;
            }
            for($i=0;$i<$this->partitions;$i++){
                $p = $path."/".$i;
                $f = fopen($p,"wb");
                $this->serializer->dump_stream4file($this->pdata[$i], new file_output_stream($f));
                unset($this->pdata[$i]);
                $this->pdata[$i]=array();
                shuffle::$DiskBytesSpilled += filesize($p);
            }
        }
        $this->spills ++;
        #TODO gc.collect()  # release the memory as much as possible
        shuffle::$MemoryBytesSpilled += max($used_memory - memory_get_usage()/1024/1024, 0) << 20;
    }


    function items(){
        if(sizeof($this->pdata)==0 && $this->spills==0) {#如果硬盘没数据
            return $this -> data;
        }
        return $this->external_items();
    }

    function external_items(){
        #""" Return all partitioned items as iterator """
        if(sizeof($this->data)!=0){
            file_put_contents("/home/".get_current_user()."/php_worker.txt", "error!!!\n", FILE_APPEND);
        }
        $haveData = False; #pdata要有数据
        foreach($this->pdata as $value){
            if($value!=null||sizeof($value)!=0){
                $haveData=True;
            }
        }
        if($haveData) {
            $this->spill();
        }
        # disable partitioning and spilling when merge combiners from disk
        $this->pdata = array();

        try {
            for($i=0; $i<$this->partitions ;$i++){
                $c=0;
                foreach($this->merged_items($i) as $k=>$v) {
                    yield $k=>$v;#自己拍脑袋出来的，竟然可以，不错
                }
                unset($this->data);

                # remove the merged partition
              /*  for($j=0;$j<$this->spills;$j++){
                   # $path = $this->get_spill_dir($j);#TODO
                    $path="/home/".get_current_user()."/php_temp/";
                    fclose($path.$i);
                }*/
            }
        }finally {
      #      $this->cleanup();
        }

    }

    function merged_items($pindex){
        $this->data = array();
        $limit = $this->next_limit();

     #   for($i = 0; $i < $this->spills ;$i++) {
            #$path = $this->get_spill_dir($i);#TODO
            $path="/home/".get_current_user()."/php_temp/";
            $p = $path . "/" . $pindex;
            $f = fopen($p, "rb");
            $iter = $this->deserializer->load_stream4file(new file_input_stream($f));
            $this->mergeCombiners($iter, -1);
            /*
            if ($this->scale * $this->partitions < $this->MAX_TOTAL_PARTITIONS
                && $i < $this->spills - 1
                && memory_get_usage() / 1024 / 1024 > $limit
            ) {
                unset($this->data);  # will read from disk again
                #     gc.collect()  # release the memory as much as possible
                return $this->recursive_merged_items($pindex);
            }*/
      #  }
        if($this->data!=null){
            return $this->data;
        }else{
            return array();
        }

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
        for($i=0;$i<$this->partitions;$i++){
            $path="/home/".get_current_user()."/php_temp/";
            $p = $path."/".$i;
            unlink($p);
        }
    }
}

class ExternalGroupBy extends ExternalMerger
{
  static  $SORT_KEY_LIMIT = 1000;



}