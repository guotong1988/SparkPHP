<?php
use org\apache\spark\streaming\api\php\PhpTransformFunction as PhpTransformFunction;
use org\apache\spark\streaming\api\php\PhpTransformFunctionSerializerInterface as PhpTransformFunctionSerializerInterface;
$spark_php_home = substr(__DIR__,0,strrpos(__DIR__,"/")-3);
require $spark_php_home.'src/vendor/autoload.php';
require($spark_php_home."/lib/Java.inc");
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
function call($milliseconds, $jrdds,$size){




    $this->failure=null;
//    try {
        if ($this->ctx == null) {
          $this->ctx = streaming_context::$activeContext;
        }
        if($this->ctx==null and $this->ctx->jsc==null){
            return;
        }


        $rdds = array();


    #    $sizeVal = java_values($size);
        file_put_contents("/home/".get_current_user()."/php_printer111.txt", "start!!!\n", FILE_APPEND);
        file_put_contents("/home/".get_current_user()."/php_printer111.txt",$size."-----\n", FILE_APPEND);
        file_put_contents("/home/".get_current_user()."/php_printer111.txt",gettype($size)."-----\n", FILE_APPEND);
        file_put_contents("/home/".get_current_user()."/php_printer111.txt",$jrdds->get(0)."---()()---\n", FILE_APPEND);
        //   file_put_contents("/home/" . get_current_user() . "/php_printer111.txt", var_export($size,TRUE) . "---\n", FILE_APPEND);

        //file_put_contents("/home/".get_current_user()."/php_printer111.txt",$size->intValue()."-----\n", FILE_APPEND);

        if(java_cast($size,"integer")==2){

            file_put_contents("/home/".get_current_user()."/php_printer111.txt",$jrdds->get(0)."=====\n", FILE_APPEND);
            file_put_contents("/home/".get_current_user()."/php_printer111.txt",$jrdds->get(1)."=====\n", FILE_APPEND);

            if($jrdds->get(0).""==""){
                array_push($rdds,null);
            }else{
                array_push($rdds, new rdd($jrdds->get(0), $this->ctx, new utf8_deserializer()));
            }

            if($jrdds->get(1).""==""){
                array_push($rdds,null);
            }else{
                array_push($rdds, new rdd($jrdds->get(1), $this->ctx, new utf8_deserializer()));
            }

        }
        if(java_cast($size,"integer")==1){

            file_put_contents("/home/".get_current_user()."/php_printer111.txt",$jrdds->get(0)."=====\n", FILE_APPEND);

            if(is_array($jrdds->get(0))){
                file_put_contents("/home/".get_current_user()."/php_printer111.txt",gettype($jrdds->get(0))."=====\n", FILE_APPEND);
            }

            if($jrdds->get(0).""=="" || is_array($jrdds->get(0))){
                array_push($rdds,null);
            }else{
                array_push($rdds, new rdd($jrdds->get(0), $this->ctx, new utf8_deserializer()));
            }
        }

/*
        for($i=0;$i<$size->intValue();$i++) { #原来是这样调通的
//            file_put_contents("/home/".get_current_user()."/php_printer111.txt",intval($size."")."\n", FILE_APPEND);
//            file_put_contents("/home/".get_current_user()."/php_printer111.txt",java_is_null($jrdds->get($i))."~~~~\n", FILE_APPEND);
//            file_put_contents("/home/".get_current_user()."/php_printer111.txt",gettype(java_is_null($jrdds->get($i)))."~~??~~\n", FILE_APPEND);

           // file_put_contents("/home/" . get_current_user() . "/php_printer111.txt", $jrdds->get($i)->toString() . "~~%%%!!~~\n", FILE_APPEND);
           // file_put_contents("/home/" . get_current_user() . "/php_printer111.txt", var_export(java_is_null($jrdds->get($i)),TRUE) . "~~%%%!!~~\n", FILE_APPEND);
           // file_put_contents("/home/" . get_current_user() . "/php_printer111.txt", java_inspect($jrdds->get($i)) . "~~!!~~\n", FILE_APPEND);
            try {
                $jrdds->get($i)->toString();
                array_push($rdds, new rdd($jrdds->get($i), $this->ctx, new utf8_deserializer()));
            }catch(Exception $e){
                array_push($rdds,null);
            }

//            if(java_is_true($jrdds->get($i))) {
//         //       file_put_contents("/home/" . get_current_user() . "/php_printer111.txt", java_inspect($jrdds->get($i)) . "~~!!~~\n", FILE_APPEND);
//                try {
//                    array_push($rdds, new rdd($jrdds->get($i), $this->ctx, new utf8_deserializer()));
//                }catch(Exception $e){
//                    array_push($rdds,null);
//                }
//            }else{
//                array_push($rdds,null);
//            }
        }
*/
        $t =time();

 //       file_put_contents("/home/".get_current_user()."/php_printer111.txt", var_export($rdds,TRUE)."&&&???\n", FILE_APPEND);

        file_put_contents("/home/".get_current_user()."/php_printer111.txt", sizeof($rdds)."?????\n", FILE_APPEND);

        if(sizeof($rdds)==0){
            file_put_contents("/home/".get_current_user()."/php_printer111.txt", var_export($size,TRUE)."?????\n", FILE_APPEND);
            array_push($rdds,null);
        }

//        $saveAsTextFile = function ($t,$rdds){
//            foreach($rdds as $rdd) {
//                if($rdd!=null) {
//                    $rdd->saveAsTextFile2("/home/gt/php_tmp/");
//                }
//            }
//        };
//        $r = $saveAsTextFile($t,$rdds);

        if(sizeof($rdds)==1) {
          //  foreach ($rdds as $rdd) {#应该是DStream->foreachRDD的核心
//                if($rdds[0]!=null) {

                    $temp = $this->func;
//                    $s = new Serializer();
//                    $str = $s->serialize($temp);
//                    file_put_contents("/home/".get_current_user()."/php_printer111.txt", $str."---!!!???\n", FILE_APPEND);

                    $r = $temp($t, $rdds[0]);

                    //file_put_contents("/home/".get_current_user()."/php_printer111.txt", var_export($r)."---!!!!!!???\n\n\n", FILE_APPEND);


                    if ($r != null) {
                        return $r->jrdd;
                    }
//                }
          //  }
        }elseif(sizeof($rdds)==2){
            $temp = $this->func;
//
//            $s = new Serializer();
//            $str = $s->serialize($temp);
//            file_put_contents("/home/".get_current_user()."/php_printer111.txt", $str."***---!!!???\n", FILE_APPEND);

            $r = $temp($t, $rdds[0],$rdds[1]);

          //  file_put_contents("/home/".get_current_user()."/php_printer111.txt", $r->jrdd."%%%---!!!!!!???\n\n\n", FILE_APPEND);


            if ($r != null) {
                return $r->jrdd;
            }
        }

//    }catch(Exception $e){
//        $this->failure=$e->getMessage();
//      //  file_put_contents("/home/gt/php_worker.txt",var_export($e,TRUE)."here3!\n", FILE_APPEND);
//    }
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
//        file_put_contents("/home/".get_current_user()."/php_printer1.txt", gettype($id)."!!!\n", FILE_APPEND);
//        file_put_contents("/home/".get_current_user()."/php_printer1.txt", $id."!!!\n", FILE_APPEND);
//        file_put_contents("/home/".get_current_user()."/php_printer1.txt", gettype(dstream::$theFunc)."!!!\n", FILE_APPEND);
            return "";

            $s = new Serializer();

            $str = $s->serialize(dstream::$theFunc);

            return $str;
    #    return unpack("C*",$str);
    }

    function loads($data){
        file_put_contents("/home/".get_current_user()."/php_printer2.txt", $data."!!!\n", FILE_APPEND);

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