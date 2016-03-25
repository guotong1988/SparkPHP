<?php

error_reporting(E_ERROR | E_WARNING | E_PARSE);
set_error_handler(function ($error, $error_string, $filename, $line, $symbols)
{
    file_put_contents("/home/gt/php_worker8.txt", $error." ".$filename." ".$line." ".$error_string. "\n",FILE_APPEND);
});






