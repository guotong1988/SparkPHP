<?php
$in = fopen('php://stdin','r');

while(!feof($in))
{
    $temp = explode(" ",fgets($in));
    for ($i=0;$i<count($temp);$i++){
        printf("%s\n",$temp[$i]);  
    }	
}
?>

