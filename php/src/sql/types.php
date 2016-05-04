<?php


function need_converter($dataType)
{
}

function create_converter($dataType){
    if  (!need_converter($dataType)) {
        return function($x){return $x;};
    }

}