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
require($spark_php_home . "src/rddsampler.php");
require($spark_php_home . "src/sql/types.php");
require($spark_php_home . "src/sql/dataframe.php");


set_include_path($spark_php_home."example/");


error_reporting(E_ERROR | E_WARNING | E_PARSE);
set_error_handler('displayErrorHandler');
function displayErrorHandler($error, $error_string, $filename, $line, $symbols)
{
    file_put_contents("/home/gt/php_worker16.txt", $error." ".$filename." ".$line." ".$error_string. "\n",FILE_APPEND);
}






$worker_main = function($sock)
{
    file_put_contents("/home/gt/php_daemon.txt",  "0here!\n", FILE_APPEND);
    global $END_OF_DATA_SECTION;
    global $PHP_EXCEPTION_THROWN;
    global $TIMING_DATA;
    global $END_OF_STREAM;
    global $NULL;

    global $spark_php_home;
    $in_stream = new sock_input_stream($sock);
    $out_stream = new sock_output_stream($sock);

    file_put_contents("/home/gt/php_daemon.txt",  "1here!\n", FILE_APPEND);

    $split_index = $in_stream->read_int();

    file_put_contents("/home/gt/php_daemon.txt",  "2here!\n", FILE_APPEND);

    if ($split_index == -1) {  # for unit tests
    }
    $version = $in_stream->read_utf();
    if ($version != "") {
    }


    shuffle::$DiskBytesSpilled = 0;
    shuffle::$MemoryBytesSpilled = 0;
#unset(Accumulator::$accumulatorRegistry);
    accumulator::$accumulatorRegistry = array();


    $spark_files_dir = $in_stream->read_utf();
    $spark_files = new spark_files();
    $spark_files->is_running_on_worker = True;
    $spark_files->root_directory = $spark_files_dir;

    file_put_contents($spark_php_home . "php_worker.txt", $spark_files_dir . "---2here!\n", FILE_APPEND);

#  add_path(spark_files_dir)
    $num_python_includes = $in_stream->read_int();
    for ($i = 0; $i < $num_python_includes; $i++) {
        $filename = $in_stream->read_utf();
        #add_path(os.path.join(spark_files_dir, filename))

        file_put_contents($spark_php_home . "php_worker.txt", $filename . "===3here!\n", FILE_APPEND);
    }

    $broadcast = new broadcast();
    $num_broadcast_variables = $in_stream->read_int();
    for ($i = 0; $i < $num_broadcast_variables; $i++) {
        $bid = $in_stream->read_long();
        if ($bid >= 0) {
            $path = $in_stream->read_utf();
            $broadcast->broadcastRegistry[$bid] = new broadcast(null, null, $path);
        } else {
            $bid = -$bid - 1;
            #     $broadcast-> broadcastRegistry->pop(bid);
        }
    }

#unset(Accumulator::$accumulatorRegistry);
    accumulator::$accumulatorRegistry = array();


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

    if ($profiler) {
        $func_name = 'process';
        $profiler->profile($func_name);
    } else {
        file_put_contents($spark_php_home . "php_worker.txt", $split_index . "here!\n", FILE_APPEND);
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

        file_put_contents($spark_php_home . "php_worker.txt", $split_index . " -------------- \n", FILE_APPEND);

        $temp3 = $func($split_index, $iterator);#分布式计算
        file_put_contents($spark_php_home . "php_worker.txt", "\n" . $split_index . "output!\n", FILE_APPEND);


//    foreach ($temp3 as $key => $element) {
//        file_put_contents($spark_php_home."php_worker.txt",var_export($temp3,True).$key."---------".var_export($element,True)."++++++++++\n", FILE_APPEND);
//    }

        $serializer->dump_stream($temp3, $out_stream);#显然是返回计算结果
    }


    report_times($out_stream, time(), time(), time());

    $out_stream->write_long(shuffle::$MemoryBytesSpilled);
    $out_stream->write_long(shuffle::$DiskBytesSpilled);

    $out_stream->write_int($END_OF_DATA_SECTION);
    $out_stream->write_int(sizeof(accumulator::$accumulatorRegistry));

    foreach (accumulator::$accumulatorRegistry as $aid => $accum) {
        $temp = array();
        $temp[0] = $aid;
        $temp[1] = $accum;
        $temp2 = serialize($temp);
        $out_stream->write_utf($temp2);
    }
    # check end of stream
    if ($in_stream->read_int() == $END_OF_STREAM) {
        $out_stream->write_int($END_OF_STREAM);
    } else {
        # write a different value to tell JVM to not reuse this worker
        $out_stream->write_int($END_OF_DATA_SECTION);
    }

};























$sock = socket_create(AF_INET, SOCK_STREAM, SOL_TCP);
socket_bind ($sock, "127.0.0.1", 0);
socket_getsockname($sock, $socket_address, $socket_port);
socket_listen($sock);


$stdout = fopen('php://stdout','w');
fwrite($stdout,pack('N', $socket_port));

$reuse = 1;

while(true){

    $temp = socket_accept($sock);

    $nPID = pcntl_fork(); // 创建子进程
    if ($nPID == 0) {// 子进程过程
        file_put_contents($spark_php_home . "php_worker2.txt",  " -------------- \n", FILE_APPEND);
        # Acknowledge that the fork was successful
        $out_stream = new sock_output_stream($sock);
        $out_stream->write_int(getmypid());
        while(true) {
            file_put_contents($spark_php_home . "php_worker2.txt",  " ?????? \n", FILE_APPEND);
            global $worker_main;
            $worker_main($sock);
            file_put_contents($spark_php_home . "php_worker2.txt",  " !!!!!! \n", FILE_APPEND);
            if (!$reuse) {
                # wait for closing
                try {
                    while(socket_recv($sock,$buf,1024,0)){
                    }
                } catch (Exception $e) {
                }
                break;
            }
        }
        file_put_contents("/home/gt/php_daemon.txt",  "---1here!\n", FILE_APPEND);
        exit;
    }else{
     #   socket_close($sock);
     #   exit;
    }

}

#TODO stop all