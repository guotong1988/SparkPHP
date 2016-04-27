<?php
use org\apache\spark\streaming\api\php\PhpTransformFunction as PhpTransformFunction;


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
        for($i=0;$i<sizeof($jrdds);$i++) {
            $rdd_temp = $jrdds->get($i);
            if($rdd_temp!=null) {
                array_push($rdds, $rdd_wrap_func($rdd_temp, $this->ctx, $this->deserializer));
            }
        }
        $t =time();

        /*$saveAsTextFile = function ($t,$rdds){
            foreach($rdds as $rdd) {
                $rdd->saveAsTextFile("/home/gt/php_tmp/");
            }
        };
        $r = $saveAsTextFile($t,$rdds);
        */

        foreach($rdds as $rdd){
            $temp = $this->func;
            $r = $temp($t,$rdd);
            if($r!=null){
                return $r->jrdd;
            }
        }
    }catch(Exception $e){
        $this->failure=$e->getMessage();
        file_put_contents("/home/gt/php_worker.txt",$e->getMessage()."here3!\n", FILE_APPEND);
    }
}

function getLastFailure()
{
    return $this->failure;
}
}