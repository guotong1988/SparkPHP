<?php
$temp = __FILE__;
$spark_php_home = substr($temp,0,strrpos($temp,"/")-3);
require($spark_php_home . "src/sock_output_stream.php");
require($spark_php_home . "src/sock_input_stream.php");
require($spark_php_home . "src/serializer.php");
require($spark_php_home . "src/shuffle.php");
require($spark_php_home . "src/accumulators.php");
require($spark_php_home . "src/files.php");
require($spark_php_home . "src/broadcast.php");
require($spark_php_home . "src/my_iterator.php");

require 'vendor/autoload.php';
use SuperClosure\Serializer;

error_reporting(E_ERROR | E_WARNING | E_PARSE);
set_error_handler('displayErrorHandler');
function displayErrorHandler($error, $error_string, $filename, $line, $symbols)
{
    $error_no_arr = array(1=>'ERROR', 2=>'WARNING', 4=>'PARSE', 8=>'NOTICE', 16=>'CORE_ERROR', 32=>'CORE_WARNING', 64=>'COMPILE_ERROR', 128=>'COMPILE_WARNING', 256=>'USER_ERROR', 512=>'USER_WARNING', 1024=>'USER_NOTICE', 2047=>'ALL', 2048=>'STRICT');
    if(in_array($error,array(1,2,4)))
    {
        $msg=$error_string;
        dieByError($msg);
    }
}

//显示错误信息
function dieByError($msg)
{
    file_put_contents("/home/gt/php_worker4.txt", $msg. "\n",FILE_APPEND);

 #   exit();
}

$stdin = fopen('php://stdin','r');
$jvm_worker_port = fgets($stdin);
$sock = socket_create ( AF_INET, SOCK_STREAM, SOL_TCP );
if ($sock == false) {
    file_put_contents($spark_php_home."php_worker.txt", "socket_create()失败:" . socket_strerror(socket_last_error($sock)) . "\n",FILE_APPEND);
    echo "socket_create()失败:" . socket_strerror(socket_last_error($sock)) . "\n";
}else {
    file_put_contents($spark_php_home."php_worker.txt", "socket_create()成功".$jvm_worker_port."\n",FILE_APPEND);
    echo "socket_create()成功\n";
}
$result =socket_connect ( $sock, '127.0.0.1', (int)$jvm_worker_port );
if ($result == false) {
    file_put_contents($spark_php_home."php_worker.txt","socket_connect()失败:" . socket_strerror(socket_last_error($sock)) . "\n", FILE_APPEND);
    echo "socket_connect()失败:" . socket_strerror(socket_last_error($sock)) . "\n";
}else {
    file_put_contents($spark_php_home."php_worker.txt", "socket_connect()成功\n", FILE_APPEND);
    echo "socket_connect()成功\n";
}

#special_lengths
$END_OF_DATA_SECTION = -1;
$PHP_EXCEPTION_THROWN = -2;
$TIMING_DATA = -3;
$END_OF_STREAM = -4;
$NULL = -5;

function report_times(sock_output_stream $out_stream, $boot, $init, $finish)
{
    global $TIMING_DATA;
    $out_stream->write_int($TIMING_DATA);
    $out_stream->write_long((int)(1000 * $boot));
    $out_stream->write_long((int)(1000 * $init));
    $out_stream->write_long((int)(1000 * $finish));

    file_put_contents("/home/gt/php_worker.txt", "成功".$boot."\n",FILE_APPEND);
}

$in_stream = new sock_input_stream($sock);
$out_stream = new sock_output_stream($sock);
$split_index =$in_stream-> read_int();

file_put_contents($spark_php_home."php_worker.txt", "首次read_int()成功".$split_index."\n", FILE_APPEND);

if($split_index == -1) {  # for unit tests
}
$version = $in_stream->read_utf();
if($version != ""){
}

file_put_contents($spark_php_home."php_worker.txt", "首次read_utf()成功".$version."\n", FILE_APPEND);

$shuffle = new shuffle();
$shuffle -> DiskBytesSpilled = 0;
$shuffle -> MemoryBytesSpilled = 0;
$accumulator = new Accumulator();
unset($accumulator->accumulatorRegistry);
$spark_files_dir = $in_stream->read_utf();
$spark_files= new spark_files();
$spark_files->is_running_on_worker=True;
$spark_files->root_directory=$spark_files_dir;
#  add_path(spark_files_dir)
$num_python_includes = $in_stream->read_int();
for($i=0;$i<$num_python_includes;$i++){
    $filename = $in_stream->read_utf();
    #add_path(os.path.join(spark_files_dir, filename))
}

file_put_contents($spark_php_home."php_worker.txt", "here1\n", FILE_APPEND);
$broadcast = new broadcast();
$num_broadcast_variables = $in_stream->read_int();
for($i=0;$i<$num_broadcast_variables;$i++) {
     $bid = $in_stream->read_long();
     if($bid >= 0) {
          $path = $in_stream->read_utf();
          $broadcast0->broadcastRegistry[$bid] = new broadcast(null,null,$path);
     }else{
          $bid = -$bid - 1;
     #     $broadcast-> broadcastRegistry->pop(bid);
     }
}
unset($accumulator->accumulatorRegistry);

file_put_contents($spark_php_home."php_worker.txt", "here2\n", FILE_APPEND);
$s = new Serializer();
$str = $in_stream->read_utf();
file_put_contents($spark_php_home."php_worker.txt", "here3".$str."\n", FILE_APPEND);
$func = $s->unserialize($str);#unserialize方法参数是serialized的string


file_put_contents($spark_php_home."php_worker.txt", "here4".gettype($func)."\n", FILE_APPEND);


$profiler = null;
$deserializer = new utf8_deserializer();
$serializer = new utf8_serializer();



function process()
{
    global $deserializer;
    global $in_stream;
    global $serializer;
    global $out_stream;
    global $split_index;
    global $func;
    global $spark_php_home;
    file_put_contents($spark_php_home."php_worker.txt", "here5a\n", FILE_APPEND);


    $iterator = new my_iterator($deserializer->load_stream($in_stream));


    foreach($iterator as $element) {
        file_put_contents($spark_php_home."php_worker.txt", "here6".$element."\n", FILE_APPEND);
    }

    $serializer -> dump_stream($func($split_index, $iterator), $out_stream);#显然是返回计算结果
}

if($profiler) {
    $func_name = 'process';
    $profiler->profile($func_name);
}else {
    file_put_contents($spark_php_home."php_worker.txt", "here5\n", FILE_APPEND);
    $iterator = new my_iterator($deserializer->load_stream($in_stream));

   # $temp2 = $in_stream->read_utf2();
   # file_put_contents($spark_php_home."php_worker.txt", "here5b ".$temp2."\n", FILE_APPEND);
    foreach($iterator as $element) {
        file_put_contents($spark_php_home."php_worker.txt", "here6".$element."\n", FILE_APPEND);
    }

    try {
        $temp3 = $func($split_index, $iterator);
    }catch(Exception $e){
        file_put_contents($spark_php_home."php_worker.txt", "here6b ".$e->getMessage()."\n", FILE_APPEND);
    }

    file_put_contents($spark_php_home."php_worker.txt", "here7 ".gettype($temp3)."\n", FILE_APPEND);

    foreach($temp3 as $element) {
        file_put_contents($spark_php_home."php_worker.txt", "here7b ".$element."\n", FILE_APPEND);
    }

    $serializer -> dump_stream($temp3, $out_stream);#显然是返回计算结果
}




report_times($out_stream,time(),time(),time());

$out_stream->write_long($shuffle->MemoryBytesSpilled);
$out_stream->write_long($shuffle->DiskBytesSpilled);

$out_stream->write_int($END_OF_DATA_SECTION);
$out_stream->write_int(sizeof($accumulator->accumulatorRegistry));


foreach($accumulator->accumulatorRegistry as $aid=>$accum){
       $temp = array();
       $temp[0]=$aid;
       $temp[1]=$accum;
       $temp2 = serialize($temp);
       $out_stream->write_utf($temp2);
}
    # check end of stream
if($in_stream->read_int() == $END_OF_STREAM){
        $out_stream->write_int($END_OF_STREAM);
} else {
        # write a different value to tell JVM to not reuse this worker
        $out_stream->write_int($END_OF_DATA_SECTION);
}