<?php
require("php_call_java.php");

class context {

    var $php_call_java;#类似pyspark里的gateway
    var $jvm;


    function context(){
        echo "kkkkkk";
        $this->php_call_java = new php_call_java();
    }

    function parallelize($data, $numSlices){
        if(is_array($data)){

        }
    }

    function text_file($filePath,$numSlices){

    }

}

