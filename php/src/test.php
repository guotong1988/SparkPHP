<?php
require 'vendor/autoload.php';
use SuperClosure\Serializer;
$temp = __FILE__;
$spark_php_home = substr($temp,0,strrpos($temp,"/")-3);


function mapPartitions(callable $f, $preservesPartitioning=False)
{
    return function ($iterator) use ($f){
            return $f($iterator);
        };
}


function fold($zeroValue, $op)
{

      return  mapPartitions(
          function ($iterator) use ($zeroValue, $op) {
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

}

$s=new Serializer();

echo $s->serialize(fold(0,1));
echo "\n";
$a = array();
array_push($a,1);
array_push($a,2);
array_push($a,3);

$iter = new my_iterator($a);
$temp = fold(0,1);
$result = $temp($a);
foreach($result as $ele){
    echo $ele;
    echo "\n";
}