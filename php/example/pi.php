<?php
require("/home/gt/spark/php/src/context.php");

$sc = new context();


$data = range(1,10);
$sc->parallelize($data,2);