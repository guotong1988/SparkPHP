<?php
require("php_call_java.php");
require("conf.php");
class context {

    var $php_call_java;#类似pyspark里的gateway
    var $jvm;
    var $jsc;#JavaSparkContext
    var $conf;

    function context(){
        echo "context构造方法";
        $this->ensure_initialized();
        $this->do_init();

        $this->jsc = $this->initialize_context($this->conf->jconf);
    }


    function do_init(){
        $this->conf =new conf($this->jvm,null);
    }

    function ensure_initialized(){
        #TODO synchronized
        if($this->php_call_java==null) {
            $this->php_call_java =new php_call_java();
            $this->jvm = $this->php_call_java->jvm;
        }
    }

    function initialize_context($jconf){
        return $this->jvm->JavaSparkContext($jconf);
    }

    function parallelize($data, $numSlices){
        if(is_array($data)){
        }
    }

    function text_file($filePath,$numSlices){

    }


}

