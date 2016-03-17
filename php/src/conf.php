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
        return $this;
    }

    # Set master URL to connect to
    function set_master($value){
        $this->jconf->setMaster($value);
    }

    function set_app_name($value){
        $this->jconf->setAppName($value);
    }

    #Set path where Spark is installed on worker nodes.
    function set_spark_home($value){
        $this->jconf->setSparkHome($value);
    }

    function set_executor_env($key=null,$value=null,$pairs=null){
        if(($key!=null and $pairs!=null) or ($key==null and $pairs==null)){
            throw new Exception("Either pass one key-value pair or a list of pairs");
        }elseif($key!=null) {
            $this->jconf->setExecutorEnv($key, $value);
        }
        elseif($pairs!=null) {
        #TODO
        }
        return $this;
    }

    function get($key, $defaultValue=null)
    {
        if($defaultValue==null) {
            if (!$this->jconf->contains($key)) {
                return null;
            }
            return $this->jconf->get($key);
        }else{
            return $this->jconf->get($key, $defaultValue);
        }
    }

    function set_if_missing($key, $value)
    {
        if($this->get($key)==null) {
            $this->set($key, $value);
        }
        return $this;
    }

    function getAll()
    {
        $pairs = array();
        #TODO
        return $pairs;
    }
}