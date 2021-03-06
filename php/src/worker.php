<?php
$temp = __FILE__;
$spark_php_home = substr($temp,0,strrpos($temp,"/")-3);


/*
require("php/src/sock_output_stream.php");
require("php/src/sock_input_stream.php");
require("php/src/serializers.php");
require("php/src/shuffle.php");
require("php/src/accumulators.php");
require("php/src/files.php");
require("php/src/broadcast.php");
require("php/src/rddsampler.php");
*/

require 'vendor/autoload.php';
use SuperClosure\Serializer;


error_reporting(E_ERROR | E_WARNING | E_PARSE);
set_error_handler('displayErrorHandler');
function displayErrorHandler($error, $error_string, $filename, $line, $symbols)
{
    file_put_contents("/home/gt/php_worker15.txt", $error." ".$filename." ".$line." ".$error_string. "\n",FILE_APPEND);
}


$stdin = fopen('php://stdin','r');
$jvm_worker_port = fgets($stdin);

$php_path_on_yarn =fgets($stdin);

#第一个是原串,第二个是部份串
function endWith($haystack, $needle) {

    $length = strlen($needle);
    if($length == 0)
    {
        return true;
    }
    return (substr($haystack, -$length) === $needle);
}

function read_all_dir ( $dir )
{
    $result = array();
    $handle = opendir($dir);
    if ( $handle )
    {
        while ( ( $file = readdir ( $handle ) ) !== false )
        {
            if ( $file != '.' && $file != '..')
            {
                $cur_path = $dir . DIRECTORY_SEPARATOR . $file;
                if ( is_dir ( $cur_path ) )
                {
                    $result['dir'][$cur_path] = read_all_dir ( $cur_path );
                }
                else
                {
                    $result['file'][] = $cur_path;
                    if(endWith($cur_path,"php")){
                        file_put_contents("/home/gt/php_worker16.txt", file_exists($cur_path). " ?\n",FILE_APPEND);
                        require_once($cur_path);
                        file_put_contents("/home/gt/php_worker16.txt", $cur_path. " !\n",FILE_APPEND);
                    }
                }
            }
        }
        closedir($handle);
    }
    return $result;
}




if($php_path_on_yarn=="NULL\n")
{
    require($spark_php_home . "src/sock_output_stream.php");
    require($spark_php_home . "src/sock_input_stream.php");
    require($spark_php_home . "src/serializers.php");
    require($spark_php_home . "src/shuffle.php");
    require($spark_php_home . "src/accumulators.php");
    require($spark_php_home . "src/files.php");
    require($spark_php_home . "src/broadcast.php");
    require($spark_php_home . "src/rddsampler.php");
    require($spark_php_home . "src/sql/types.php");
    require($spark_php_home . "src/sql/dataframe.php");
    set_include_path($spark_php_home."example/");
 #   read_all_dir($spark_php_home."example");
}else{

    $php_path_on_yarn = str_replace("\n","",$php_path_on_yarn);
    require($php_path_on_yarn . "src/sock_output_stream.php");
    require($php_path_on_yarn . "src/sock_input_stream.php");
    require($php_path_on_yarn . "src/serializers.php");
    require($php_path_on_yarn . "src/shuffle.php");
    require($php_path_on_yarn . "src/accumulators.php");
    require($php_path_on_yarn . "src/files.php");
    require($php_path_on_yarn . "src/broadcast.php");
    require($php_path_on_yarn . "src/rddsampler.php");
    require($php_path_on_yarn . "src/sql/types.php");
    require($php_path_on_yarn . "src/sql/dataframe.php");
    set_include_path($php_path_on_yarn."example/");
 #   read_all_dir($php_path_on_yarn."example");
}

$sock = socket_create ( AF_INET, SOCK_STREAM, SOL_TCP );
if ($sock == false) {
    file_put_contents($spark_php_home."php_worker.txt", "socket_create()失败:" . socket_strerror(socket_last_error($sock)) . "\n",FILE_APPEND);
}else {
    file_put_contents($spark_php_home."php_worker.txt", "socket_create()成功".$jvm_worker_port."\n",FILE_APPEND);
}

$result =socket_connect ( $sock, '127.0.0.1', intval($jvm_worker_port) );
if ($result == false) {
    file_put_contents($spark_php_home."php_worker.txt","socket_connect()失败:" . socket_strerror(socket_last_error($sock)) . "\n", FILE_APPEND);
}else {
    file_put_contents($spark_php_home."php_worker.txt", "socket_connect()成功\n", FILE_APPEND);
}

#special_lengths
$END_OF_DATA_SECTION = -1;
$PHP_EXCEPTION_THROWN = -2;
$TIMING_DATA = -3;
$END_OF_STREAM = 4294967292;
$NULL = -5;

function report_times(sock_output_stream $out_stream, $boot, $init, $finish)
{
    global $TIMING_DATA;
    $out_stream->write_int($TIMING_DATA);
    $out_stream->write_long((int)(1000 * $boot));
    $out_stream->write_long((int)(1000 * $init));
    $out_stream->write_long((int)(1000 * $finish));

}

$in_stream = new sock_input_stream($sock);
$out_stream = new sock_output_stream($sock);
$split_index =$in_stream-> read_int();


if($split_index == -1) {  # for unit tests
}
$version = $in_stream->read_utf();
if($version != ""){
}


shuffle::$DiskBytesSpilled = 0;
shuffle::$MemoryBytesSpilled = 0;
#unset(Accumulator::$accumulatorRegistry);
accumulator::$accumulatorRegistry=array();


$spark_files_dir = $in_stream->read_utf();
$spark_files= new spark_files();
$spark_files->is_running_on_worker=True;
$spark_files->root_directory=$spark_files_dir;

file_put_contents($spark_php_home."php_worker.txt", $spark_files_dir."---2here!\n", FILE_APPEND);

#  add_path(spark_files_dir)
$num_python_includes = $in_stream->read_int();
for($i=0;$i<$num_python_includes;$i++){
    $filename = $in_stream->read_utf();
    #add_path(os.path.join(spark_files_dir, filename))

    file_put_contents($spark_php_home."php_worker.txt", $filename."===3here!\n", FILE_APPEND);
}

$broadcast = new broadcast();
$num_broadcast_variables = $in_stream->read_int();
for($i=0;$i<$num_broadcast_variables;$i++) {
     $bid = $in_stream->read_long();
     if($bid >= 0) {
          $path = $in_stream->read_utf();
          $broadcast->broadcastRegistry[$bid] = new broadcast(null,null,$path);
     }else{
          $bid = -$bid - 1;
     #     $broadcast-> broadcastRegistry->pop(bid);
     }
}

#unset(Accumulator::$accumulatorRegistry);
accumulator::$accumulatorRegistry=array();


$s = new Serializer();
$str = $in_stream->read_utf();


$func = $s->unserialize($str);#unserialize方法参数是serialized的string



$profiler = null;
$deserializer = new utf8_deserializer();
$serializer = new utf8_serializer();



function process()
{
    /*
    global $deserializer;
    global $in_stream;
    global $serializer;
    global $out_stream;
    global $split_index;
    global $func;
    global $spark_php_home;

    $iterator = $deserializer->load_stream2($in_stream);

    $serializer -> dump_stream($func($split_index, $iterator), $out_stream);#显然是返回计算结果
    */
}

if($profiler) {
    $func_name = 'process';
    $profiler->profile($func_name);
}else {
    file_put_contents($spark_php_home."php_worker.txt", $split_index."here!\n", FILE_APPEND);
    $iterator = $deserializer->load_stream($in_stream);


/*
    file_put_contents($spark_php_home."php_worker.txt", $split_index." read ".$iterator->current()."\n", FILE_APPEND);
    if(is_array($iterator->current()))
    {
       foreach($iterator->current() as $e){
           file_put_contents($spark_php_home."php_worker.txt", $split_index." rrr ".$e."\n", FILE_APPEND);
       }
    }

       $i=0;
       foreach($iterator as $e) {
         file_put_contents($spark_php_home."php_worker.txt",$split_index. "input ".$e."\n", FILE_APPEND);
         if ($i>10) {break;}
           $i++;
       }
#    $iterator->rewind();
*/

    file_put_contents($spark_php_home."php_worker.txt", $split_index." -------------- \n", FILE_APPEND);

     $temp3 = $func($split_index, $iterator);#分布式计算
     file_put_contents($spark_php_home."php_worker.txt","\n".$split_index."output!\n", FILE_APPEND);


  //  foreach ($temp3 as $key => $element) {
        file_put_contents($spark_php_home."php_worker.txt",var_export($temp3,True)."++++++++++\n", FILE_APPEND);
  //  }
    if(is_array($temp3)&&sizeof($temp3)==0){}
    else {
        $serializer->dump_stream($temp3, $out_stream);#显然是返回计算结果
    }
}




report_times($out_stream,time(),time(),time());

$out_stream->write_long(shuffle::$MemoryBytesSpilled);
$out_stream->write_long(shuffle::$DiskBytesSpilled);

$out_stream->write_int($END_OF_DATA_SECTION);
$out_stream->write_int(sizeof(accumulator::$accumulatorRegistry));

foreach(accumulator::$accumulatorRegistry as $aid=>$accum){
       $temp = array();
       $temp[0]=$aid;
       $temp[1]=$accum;
       $temp2 = serialize($temp);
       $out_stream->write_utf($temp2);
}

$ttt = $in_stream->read_int();

file_put_contents($spark_php_home."php_worker.txt", $ttt."ttt-------------- \n", FILE_APPEND);
    # check end of stream
if($ttt == $END_OF_STREAM){
        $out_stream->write_int($END_OF_STREAM);
} else {
        # write a different value to tell JVM to not reuse this worker
        $out_stream->write_int($END_OF_DATA_SECTION);
}
