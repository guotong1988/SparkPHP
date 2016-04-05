<?php

class file_output_stream {
    private $file = null;

    public function __construct( $file ){
        $this->file = $file;
    }

    public function get_file(){
        return $this->file;
    }

    public function __destruct(){
    }

    function write_int($value)
    {
        $data = pack('N', $value);
        fwrite($this->file, $data);
    }

    function write_utf($value)
    {
        $this->write_int(strlen($value));
        fwrite($this->file,$value,strlen($value));
    }

}