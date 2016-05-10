<?php
class DataCleaner{

    static $produce;

    function __construct(){
        self::$produce = \Kafka\Produce::getInstance('localhost:2181', 3000);
    }

    function isDirty($dataDetail){
        if($dataDetail==null){
            return true;
        }
        if($this->checkDataDirty($dataDetail)){
            return true;
        }
        return false;
    }

    function checkDataDirty(DataDetail $dataDetail){
        $id = $dataDetail->id;
        $aoiID = $dataDetail->aoiId;
        $type = $dataDetail->type;
        $state = $dataDetail->dataState;
        switch($type){
            case "order":
                if($state=="orderadd" or $state == "orderuntake" or $state == "orderfinish") {
                    $sendValue = "status:".$dataDetail->getJson();
                    self::$produce->setMessages("topicName",0,array($sendValue));#TODO
                    self::$produce->send();
                }
                if ($state != "orderadd") {
                    return true;
                } else {
                    return false;
                }
            case "rider":
                $sendValue = "status:".$dataDetail->getJson();
                self::$produce->setMessages("topicName",0,array($sendValue));#TODO
                return true;
            case "order dubious":
                return false;
            case "rider position":
                return true;
            case "order payment":
                return false;
            case "order illegal":
                return false;
        }
    }


}