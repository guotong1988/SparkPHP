<?php
use org\apache\spark\streaming\api\php\PhpTransformFunction as PhpTransformFunction;
use org\apache\spark\streaming\api\php\PhpTransformFunctionSerializerInterface as PhpTransformFunctionSerializerInterface;
$spark_php_home = substr(__DIR__,0,strrpos(__DIR__,"/")-3);
require $spark_php_home.'src/vendor/autoload.php';
use SuperClosure\Serializer;
class TransformFunction extends PhpTransformFunction{

var $func;
var $ctx;
var $deserializer;
var $rdd_wrap_func;
var $failure;
function __construct($ctx,$func, $deserializer)
{

    $this->ctx = $ctx;
    $this->func = $func;
    $this->deserializer = $deserializer;
    $this->rdd_wrap_func = function($jrdd, $ctx, $ser){
        return new rdd($jrdd, $ctx, $ser);
    };
    $this->failure = null;

}

#streaming调用开始的地方
function call($milliseconds, $jrdds){
    $this->failure=null;
    try {
        if ($this->ctx == null) {
          $this->ctx = streaming_context::$activeContext;
        }
        if($this->ctx==null and $this->ctx->jsc==null){
            return;
        }

        $rdd_wrap_func = function($jrdd, $ctx, $ser){
            return new rdd($jrdd, $ctx, $ser);
        };
        $rdds = array();

        file_put_contents("/home/".get_current_user()."/php_printer111.txt", $jrdds->size()."!!!\n", FILE_APPEND);
        file_put_contents("/home/".get_current_user()."/php_printer111.txt", sizeof(java_values($jrdds))."---\n", FILE_APPEND);


       # for($i=0;$i<$jrdds->size();$i++) { #原来是这样调通的
        for($i=0;$i<sizeof(java_values($jrdds));$i++) {
            $rdd_temp = $jrdds->get($i);

            file_put_contents("/home/".get_current_user()."/php_printer111.txt", java_values($rdd_temp)."***\n", FILE_APPEND);
            file_put_contents("/home/".get_current_user()."/php_printer111.txt", $rdd_temp."^^^\n", FILE_APPEND);

            if($rdd_temp!=null &&  $rdd_temp."" != "") {
                array_push($rdds, $rdd_wrap_func($rdd_temp, $this->ctx, $this->deserializer));
            }else{
                array_push($rdds,null);
            }
        }
        $t =time();

        $saveAsTextFile = function ($t,$rdds){
            foreach($rdds as $rdd) {
                if($rdd!=null) {
                    $rdd->saveAsTextFile2("/home/gt/php_tmp/");
                }
            }
        };
        $r = $saveAsTextFile($t,$rdds);


        if(sizeof($rdds)==1) {
            foreach ($rdds as $rdd) {#应该是DStream->foreachRDD的核心
                $temp = $this->func;
                $r = $temp($t, $rdd);
                if ($r != null) {
                    return $r->jrdd;
                }
            }
        }else{
            $temp = $this->func;
            $r = $temp($t, $rdds[0],$rdds[1]);
            if ($r != null) {
                return $r->jrdd;
            }
        }

    }catch(Exception $e){
        $this->failure=$e->getMessage();
    }
}

function getLastFailure()
{
    return $this->failure;
}
}

#checkpoint会用
class TransformFunctionSerializer extends PhpTransformFunctionSerializerInterface{

    var $ctx;
    var $serializer;
    var $php_call_java;
    var $failure;

    function __construct($ctx, $serializer,$php_call_java=null){
        $this->ctx = $ctx;
        $this->serializer = $serializer;
        if($php_call_java==null){
            $this->php_call_java = $this->ctx->php_call_java;
        }else{
            $this->php_call_java = $php_call_java;
        }

        $this->failure = null;
    }

    function dumps($id){
        $s = new Serializer();

        $str = $s->serialize(dstream::$theFunc);

        file_put_contents("/home/".get_current_user()."/php_printer1.txt", $str."!!!\n", FILE_APPEND);
        return $str;
    #    return unpack("C*",$str);
    }

    function loads($data){
        $s = new Serializer();

        file_put_contents("/home/".get_current_user()."/php_printer2.txt", $data."!!!\n", FILE_APPEND);

        $func = $s->unserialize($data);

        file_put_contents("/home/".get_current_user()."/php_printer2.txt", $data."!!!\n", FILE_APPEND);

        return new TransformFunction($this->ctx, $func, new utf8_deserializer());
    }

    function getLastFailure(){
        return $this->failure;
    }
}