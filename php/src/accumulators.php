<?php

$accumulatorRegistry = array();

class Accumulator
{

    var $aid;
    var $value;
    var $accum_param;
    var $deserialized;

    function deserialize_accumulator($aid, $zero_value, $accum_param){
        $accum =new Accumulator($aid, $zero_value, $accum_param);
        $accum->deserialized = True;
        global $accumulatorRegistry;
        $accumulatorRegistry[$aid] = $accum;
        return $accum;
    }


    function __construct($aid, $value, $accum_param)
    {
        $this->aid = $aid;
        $this->accum_param = $accum_param;
        $this->value = $value;
        $this->deserialized = False;
        $this->accumulatorRegistry[$aid] = $this;
    }

    function __reduce__()
    {
        #"""Custom serialization; saves the zero value from our AccumulatorParam"""
        $param = $this->accum_param;
        #TODO
    }
}


class AccumulatorServer{

    var $tcp_server;
    var $server_shutdown = False;

    function shutdown()
    {
        $this->server_shutdown = True;
        #TODO
        #$tcp_server->shutdown();
        #self . server_close()
    }
}
