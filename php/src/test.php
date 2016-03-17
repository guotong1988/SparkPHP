<?php
require 'vendor/autoload.php';
use SuperClosure\Serializer;



function f2($f){
    $f();
    echo "!!!!";
    $serializer = new Serializer();
  #  serialize($f); #失败
    $serializer->serialize($f);
}

function f1() {
    return f2(

        function($a){
            echo "!!!".$a;
        }

    );
};

f1();

