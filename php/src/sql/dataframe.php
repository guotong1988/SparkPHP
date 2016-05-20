<?php
class DataFrame{

    function __construct($jdf, $sql_ctx){
        $this->jdf = $jdf;
        $this->sql_ctx = $sql_ctx;
        $this->sc = $sql_ctx->sc;
        $this->is_cached = False;
        $this->schema = null;  # initialized lazily
        $this->lazy_rdd = null;

    }

    function collect(){
    #TODO synchronized
        $port = $this->jdf->collectToPhp();
        return $this->load_from_socket($port,null);
    }

    function load_from_socket($port, $deserializer)
    {
        $sock = socket_create(AF_INET, SOCK_STREAM, SOL_TCP);
        if ($sock == false) {
            echo "socket_create()失败:" . socket_strerror(socket_last_error($sock)) . "\n";
        } else {
            echo "socket_create()成功\n";
        }

        $port = $port->intValue() . "";#不然不行，坑啊

        $result = socket_connect($sock, '127.0.0.1', intval($port));
        if ($result == false) {
            echo "socket_connect()失败:" . socket_strerror(socket_last_error($sock)) . "\n";
        } else {
            echo "socket_connect()成功\n";
        }
        $stream = new sock_input_stream($sock);
        if ($deserializer == null) {
            $deserializer = new utf8_deserializer();
        }
        $item_array = $deserializer->load_stream($stream);
        #socket_close($sock);#改成yield之后不能关了
        return $item_array;
    }

    function printSchema()
    {
        print($this->jdf->schema()->treeString());
    }

    function registerAsTable($name)
    {
        $this->registerTempTable($name);
    }

    function registerTempTable($name)
    {
        $this->jdf->registerTempTable($name);
    }

    function filter($condition){
        $jdf = $this->jdf->filter($condition);
        return new DataFrame($jdf, $this->sql_ctx);
    }

    function select($cols){
        $jdf = $this->jdf->select($this->jcols($cols));
        return new DataFrame($jdf,$this->sql_ctx);
    }

    function jcols($cols){

        $_create_column_from_name=function($name) {
            $this->sc->php_call_java->functions->col($name);
        };

        $_to_java_column = function ($col) use ($_create_column_from_name)
        {
            $jcol = $_create_column_from_name($col);
            return $jcol;
        };

        return $this->jseq($cols,$_to_java_column);
    }


    function jseq($cols, $converter=null)
    {

         $_to_seq = function($sc, $cols,callable $converter=null) {
             if($converter!=null) {
                 $temp = array();
                 foreach($cols as $e)
                 {
                     array_push($temp,$converter($e));
                 }
                 return $sc->php_call_java->PhpUtils->toSeq($temp);
             }
             return $sc->php_call_java->PhpUtils->toSeq($cols);
        };

        return $_to_seq($this->sql_ctx->sc, $cols, $converter);
    }
}