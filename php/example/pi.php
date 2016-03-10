<?php
require("../src/context.php");#TODO spark/php/example$ ../../bin/spark-submit pi.php

$sc = new context();


$data = range(1,10);
$sc->parallelize($data,2);
