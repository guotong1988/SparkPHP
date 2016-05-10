<?php

class DataDetail
{
    var $type;
    var $dataState;
    var $time;
    var $aoiId = "";
    var $expectTime = 0;
    var $id = 0;

    function __construct($type, $aoiId, $dataState, $time, $id, $expectTime)
    {
        $this->type = $type;
        $this->time = $time;
        $this->id = $id;
        $this->aoiId = $aoiId;
        $this->dataState = $dataState;
        $this->expectTime = $expectTime;
    }

    function getJson(){
        $json = array();
        global $dataTypeMap;
        global $OrderStateMap;
        $json["id"]=$this->id;
        $json["aoiId"]=$this->aoiId;
        $json["type"]=$dataTypeMap[$this->type];
        $json["timestamp"]=$this->time;
        $json["status"]=$OrderStateMap[$this->dataState];
        $json["expecttime"] =$this->expectTime;
        return json_encode($json);
    }

}