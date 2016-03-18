<?php
$temp = __FILE__;
$spark_php_home = substr($temp,0,strrpos($temp,"/")-3);
require($spark_php_home . "src/sock_output_stream.php");
require($spark_php_home . "src/sock_input_stream.php");
require($spark_php_home . "src/serializers.php");
require($spark_php_home . "src/shuffle.php");
require($spark_php_home . "src/accumulators.php");
require($spark_php_home . "src/files.php");
require($spark_php_home . "src/broadcast.php");

$stdin = fopen('php://stdin','r');
$jvm_worker_port = fgets($stdin);
$sock = socket_create ( AF_INET, SOCK_STREAM, SOL_TCP );
if ($sock == false) {
    file_put_contents($spark_php_home."php_worker.txt", "socket_create()失败:" . socket_strerror(socket_last_error($sock)) . "\n");
    echo "socket_create()失败:" . socket_strerror(socket_last_error($sock)) . "\n";
}else {
    file_put_contents($spark_php_home."php_worker.txt", "socket_create()成功\n");
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


$in_stream = new sock_input_stream($sock);
$out_stream = new sock_output_stream($sock);
$split_index =$in_stream-> read_int();

file_put_contents($spark_php_home."php_worker.txt", "首次read_int()成功".$split_index."\n", FILE_APPEND);

if($split_index == -1) {  # for unit tests
}
$utf8_deserializer = new utf8_deserializer();
$version = $utf8_deserializer->loads($in_stream);
if($version != ""){
}

file_put_contents($spark_php_home."php_worker.txt", "首次read_utf()成功".$version."\n", FILE_APPEND);

$shuffle = new shuffle();
$shuffle -> DiskBytesSpilled = 0;
$shuffle -> MemoryBytesSpilled = 0;
$accumulator = new Accumulator();
unset($accumulator->accumulatorRegistry);
$spark_files_dir = $utf8_deserializer->loads($in_stream);
$spark_files= new spark_files();
$spark_files->is_running_on_worker=True;
$spark_files->root_directory=$spark_files_dir;
#  add_path(spark_files_dir)
$num_python_includes = $in_stream->read_int();
for($i=0;$i<$num_python_includes;$i++){
    $filename = $utf8_deserializer->loads($in_stream);
    #add_path(os.path.join(spark_files_dir, filename))
}


$broadcast = new broadcast();
$num_broadcast_variables = $in_stream->read_int();
for($i=0;$i<$num_broadcast_variables;$i++) {
     $bid = $in_stream->read_long();
     if($bid >= 0) {
          $path = $utf8_deserializer->loads($in_stream);
          $broadcast0->broadcastRegistry[$bid] = new broadcast(null,null,$path);
     }else{
          $bid = -$bid - 1;
     #     $broadcast-> broadcastRegistry->pop(bid);
     }
}
unset($accumulator->accumulatorRegistry);
$temp_length = $in_stream->read_int();

file_put_contents($spark_php_home."php_worker.txt", "here\n", FILE_APPEND);

$command = unserialize($in_stream->read_fully($temp_length));#unserialize方法参数是serialized的string

file_put_contents($spark_php_home."php_worker.txt", "here\n", FILE_APPEND);

if($command instanceof broadcast) {
    $command = unserialize($command->value);
}
$func = $command[0];#"就是func"
$profiler = $command[1];
$deserializer = new utf8_deserializer();#$command[2];
$serializer = $command[3];


function process()
{
    global $deserializer;
    global $in_stream;
    global $serializer;
    global $out_stream;
    global $split_index;
    $iterator = $deserializer->load_stream($in_stream);
    #$serializer -> dump_stream(func(split_index, iterator), $out_stream);#显然是返回计算结果
}

if($profiler) {
    $func_name = 'process';
    $profiler->profile($func_name);
}else {
    process();
}


function report_times(sock_output_stream $out_stream, $boot, $init, $finish)
{
    global $TIMING_DATA;
    $out_stream->write_int($TIMING_DATA);
    $out_stream->write_long(int(1000 * $boot));
    $out_stream->write_long(int(1000 * $init));
    $out_stream->write_long(int(1000 * $finish));
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
       $out_stream->write_string($temp2);
}
    # check end of stream
if($in_stream->read_int() == $END_OF_STREAM){
        $out_stream->write_int($END_OF_STREAM);
} else {
        # write a different value to tell JVM to not reuse this worker
        $out_stream->write_int($END_OF_DATA_SECTION);
}