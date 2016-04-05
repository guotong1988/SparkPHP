<?php

class file_input_stream {
    private $file = null;

    public function __construct( $file ){
        $this->file = $file;
    }

    public function get_file(){
        return $this->file;
    }

    public function __destruct(){
    }

    function read_int()
    {
        $data = fread($this->file, 4);
        if($data==""){
            throw new Exception("end of data");
        }
        $raw = unpack('N', $data);
        $val = $raw[1] & 0xffffffff;
        return $val;
    }

    function read_utf()
    {
        $l = $this->read_int();
        return fread($this->file,$l);
    }

    function read_fully($l)
    {
        return fread($this->file,$l);
    }
}