<?php
#$spark_php_home = substr(__DIR__,0,strrpos(__DIR__,"/")-3);
require __DIR__."/dstream.php";
require __DIR__."/util.php";

require __DIR__."/Kafka.php";

class streaming_context{
    static $activeContext;
    var $sc;
    var $php_call_java;
    var $jssc;
    function __construct($spark_context, $batchDuration=null, $jssc=null){
        $this->sc = $spark_context;
        $this->php_call_java = $this->sc->php_call_java;
        if($jssc!=null){
            $this->jssc = $jssc;
        }else{
            $this->jssc = $this->initialize_context($this->sc, $batchDuration);
        }
    }

    function initialize_context($sc, $duration)
    {
        $this->ensure_initialized();
        return $this->php_call_java->java_streaming_context($sc->jsc, $this->jduration($duration));
    }

    function jduration($seconds)
    {
        return $this->php_call_java->duration(java_values($seconds * 1000));
    }

    function ensure_initialized(){
        $this->sc->ensure_initialized();
        if($this->php_call_java==null){
            $this->php_call_java=$this->sc->php_call_java;
        }
    }

    function textFileStream($directory)
    {
        return new dstream($this->jssc->textFileStream($directory), $this, new utf8_deserializer());
    }

    function start()
    {
        $this->jssc->start();
        streaming_context::$activeContext = $this;
    }

    function awaitTermination($timeout=null)
    {
        if($timeout==null) {
            $this->jssc->awaitTermination();
        }else {
            $this->jssc->awaitTerminationOrTimeout(java_values($timeout * 1000));
        }
    }

    function awaitTerminationOrTimeout($timeout){
        return $this->jssc->awaitTerminationOrTimeout(java_values($timeout * 1000));
    }

    function checkpoint($directory)
    {
        $this->jssc->checkpoint($directory);
    }
}