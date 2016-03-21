<?php

class sock_output_stream {
    private $sock = null;

    public function __construct( $sock ){
        $this->sock = $sock;
    }

    public function __destruct(){
    }

    function write_int($value)
    {
        $data = pack('N', $value);
        socket_write($this->sock, $data);
    }

    function write_long($value)
    {
        $data = pack('J', $value);
        socket_write($this->sock, $data);
    }

    function write_short($value)
    {
        $data = pack('n', $value);
        socket_write($this->sock, $data);
    }

    function write_utf($value)
    {
        $this->write_short(strlen($value));
        socket_write($this->sock,$value);
    }

    function write_utf2($value)
    {
        $this->write_int(strlen($value));
        socket_write($this->sock,$value);
    }

}