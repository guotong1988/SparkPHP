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
$END_OF_STREAM = -4;
$NULL = -5;

function report_times(sock_output_stream $out_stream, $boot, $init, $finish)
{
    global $TIMING_DATA;
    $out_stream->write_int($TIMING_DATA);
    $out_stream->write_long((int)(1000 * $boot));
    $out_stream->write_long((int)(1000 * $init));
    $out_stream->write_long((int)(1000 * $finish));

}