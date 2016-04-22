<?php

require "../php_call_java.php";


class streaming_context{

    var $sc;
    var $jvm;
    var $jssc;
    function __construct($spark_context, $batchDuration=null, $jssc=null){

        $this->sc = $spark_context;
        $this->jvm = $this->sc->php_call_java;
        if($jssc!=null){
            $this->jssc = $jssc;
        }else{
            $this->jssc = $this->initialize_context($this->sc, $batchDuration);
        }
    }

    function initialize_context($sc, $duration)
    {
        $this->ensure_initialized();
        return $this->jvm->java_streaming_context($sc->jsc, $this->jduration($duration));
    }

    function jduration($seconds)
    {
        return $this->jvm->duration(intval($seconds * 1000));
    }

    function ensure_initialized(){
        if($this->jvm==null){
            $this->jvm=new php_call_java();
        }
    }

}