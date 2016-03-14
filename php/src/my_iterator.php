<?php
class my_iterator implements Iterator {
    private $position = 0;

    private $array;

    public function __construct($given_array) {
        $this->array = $given_array;
        $this->position = 0;
    }

    public function get_array(){
        return $this->array;
    }

    function rewind() {
        $this->position = 0;
    }

    function first(){
        return $this->array[0];
    }

    function current() {
        return $this->array[$this->position];
    }

    function key() {
        return $this->position;
    }

    function next() {
        ++$this->position;
    }

    function valid() {
        return isset($this->array[$this->position]);
    }
}