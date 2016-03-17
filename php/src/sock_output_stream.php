<?php

class sock_output_stream {
    private $sock = null;

    public function __construct( $sock ){
        $this->sock = $sock;
    }

    public function __destruct(){
    }

#    function readInt($socket) {
#        $n = socket_read($socket, 4);
#        $unpacked = unpack("N", $n);
#        return $unpacked[1];
#    }

    function write_int($value) {
        socket_write($this->$sock, pack("N", $value), 4);
    }


    function write_long($value) {
        socket_write($this->$sock, pack("N", $value), 8);
    }

#    function readString($socket) {
#        $n = readInt($socket);
#        return socket_read($socket, $n);
#    }

    function write_string($value) {
        $len = strlen($value);
        writeInt($this->$sock, $len);
        socket_write($this->$sock, $value, $len);
    }
}