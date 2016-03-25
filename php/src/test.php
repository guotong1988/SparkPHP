<?php
require 'vendor/autoload.php';
use SuperClosure\Serializer;
$temp = __FILE__;
$spark_php_home = substr($temp,0,strrpos($temp,"/")-3);
require($spark_php_home . "src/shuffle.php");

function mapPartitions(callable $f, $preservesPartitioning=False)
{
    $temp = new Aggregator($f,$f,$f);
    return function ($iterator) use ($f,$temp){
            return $f($iterator);
        };
}


$fold = function ($zeroValue, $op)
{

      $f = function ($x1,$x2) {
        return $x1+$x2;
      };
      $temp = new Aggregator($f,$f,$f);
      return  mapPartitions(
          function ($iterator) use ($zeroValue, $op,$temp) {
     #     echo "!!!".$iterator->get_array()[1];

            $acc = $zeroValue;
            foreach ($iterator as $element) {
                $ADD = 1;
                #     if($op==$ADD) {
                $acc = $element + $acc;
                #    }
            }
            $temp = array();
            array_push($temp, $acc);
            return $temp;
        });

};

$s=new Serializer();
echo "!!!!!".$s->serialize($fold)."!!!!!";