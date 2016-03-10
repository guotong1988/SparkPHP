<?php

class conf {
    var $jconf;

    function conf($jconf,$php_context)
    {
        if ($jconf != null) {
            $this->jconf = $jconf;
        } else {
            $this->jconf = $php_context->jvm->SparkConf;
         }
    }

    function set($key, $value){
        $this->jconf->set($key, $value);
    }

    # Set master URL to connect to
    function setMaster($value){
        $this->jconf->setMaster($value);
    }

    function setAppName($value){
        $this->jconf->setAppName($value);
    }

    #Set path where Spark is installed on worker nodes.
    function setSparkHome($value){
        $this->jconf->setSparkHome($value);
    }

}