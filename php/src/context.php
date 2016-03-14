<?php
$temp = __FILE__;
$spark_php_home = substr($temp,0,strrpos($temp,"/")-3);
require($spark_php_home . "src/conf.php");
require($spark_php_home . "src/php_call_java.php");
require($spark_php_home . "src/rdd.php");
require($spark_php_home . "src/serializers.php");
class context {

    var $jvm;#就是php_call_java
    var $jsc;#JavaSparkContext
    var $conf;

    function context(){
        echo "context构造方法（开始）";
        $this->ensure_initialized();
        $this->do_init();
        echo "context构造方法（结束）";
    }

    function do_init(){
        $this->conf =new conf(null,$this);
        $this->jsc = $this->initialize_context($this->conf->jconf);
    }

    function ensure_initialized(){
        #TODO synchronized
        if($this->jvm==null) {
            $this->jvm = new php_call_java();
        }
    }

    function initialize_context($jconf){
        $this->jvm->JavaSparkContext->set($jconf);
        return $this->jvm->JavaSparkContext;
    }

    function parallelize($data, $numSlices){
        if(is_array($data)){
        }
    }

    function text_file($filePath,$minPartitions=null,$use_unicode=True){
        if ($minPartitions==null){
            $minPartitions=1;
        }
        $HadoopRDD = $this->jsc->textFile($filePath, $minPartitions);
        $serializers = new utf8_deserializer($use_unicode);
        return new rdd($HadoopRDD, $this, $serializers);
    }


}

