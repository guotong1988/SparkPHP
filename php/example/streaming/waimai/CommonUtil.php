<?php

class CommonUtil{

    static $LOG_FOR_MONITOR_SIGN = "REALTIMELOG";
    static $ID_SIGN = "realtime_id";
    static $STATUS_SIGN = "realtime_status";
    static $TYPE_SIGN = "realtime_type";
    static $TIMESTAMP_SIGN = "realtime_timestamp";
    static $AOIID_SIGN = "aoiid";


    static function parseMessageJson($message)
    {
        $jsonStrStartIndex = strpos($message,"realtime_data");
        $jsonString = substr($message,$jsonStrStartIndex+14);
        $jsonObject = json_decode($jsonString,true);
        $typeStr = $jsonObject[self::$TYPE_SIGN];
        global $dataTypeMap;
        $dataType = $dataTypeMap[$typeStr];
        $retDataDetail = null;
        switch($dataType){
            case "order":
                $retDataDetail = self::parseOrderStatusJson($jsonObject);
                break;
            case "rider":
                $retDataDetail = self::parseRiderStatusJson($jsonObject);
                break;
            case "order dubious":
                $retDataDetail = self::parseRiderPositionJson($jsonObject);
                break;
            case "rider position":
                $retDataDetail = self::parseRiderPositionJson($jsonObject);
                break;
            case "order payment":
                $retDataDetail = self::parsePaymentOrderJson($jsonObject);
                break;
            case "order illegal":
                $retDataDetail = self::parseIllegalOrderJson($jsonObject);
                break;
        }
        return $retDataDetail;
    }


    static function parseIllegalOrderJson($jsonArray){

        $typeStr = $jsonArray[self::$TYPE_SIGN];
        global $dataTypeMap;
        $dataType = $dataTypeMap[$typeStr];
        $time = $jsonArray[self::$TIMESTAMP_SIGN];
        $id = $jsonArray[self::$ID_SIGN];
        $aoiId = $jsonArray[self::$AOIID_SIGN];
        return new DataDetail($dataType, $aoiId, "orderillegal", $time, $id, null);

    }

    static function parsePaymentOrderJson($jsonArray){

        $typeStr = $jsonArray[self::$TYPE_SIGN];
        global $dataTypeMap;
        $dataType = $dataTypeMap[$typeStr];
        $time = $jsonArray[self::$TIMESTAMP_SIGN];
        $id = $jsonArray[self::$ID_SIGN];
        $aoiId = $jsonArray[self::$AOIID_SIGN];
        return new DataDetail($dataType, $aoiId, "orderpayment", $time, $id, null);

    }

    static function parseDubiousOrderJson($jsonArray){

        $typeStr = $jsonArray[self::$TYPE_SIGN];
        global $dataTypeMap;
        $dataType = $dataTypeMap[$typeStr];
        $time = $jsonArray[self::$TIMESTAMP_SIGN];
        $id = $jsonArray[self::$ID_SIGN];
        $aoiId = $jsonArray[self::$AOIID_SIGN];
        return new DataDetail($dataType, $aoiId, "orderdubious", $time, $id, null);

    }


    static function parseRiderPositionJson($jsonArray){
        $typeStr = $jsonArray[self::$TYPE_SIGN];
        global $dataTypeMap;
        $dataType = $dataTypeMap[$typeStr];
        $time = $jsonArray[self::$TIMESTAMP_SIGN];
        $id = $jsonArray[self::$ID_SIGN];
        $aoiId = $jsonArray[self::$AOIID_SIGN];
        return new DataDetail($dataType, $aoiId, null, $time, $id, null);
    }

    static function parseRiderStatusJson($jsonArray){
        $typeStr = $jsonArray[self::$TYPE_SIGN];
        global $dataTypeMap;
        $dataType = $dataTypeMap[$typeStr];
        $time = $jsonArray[self::$TIMESTAMP_SIGN];
        $id = $jsonArray[self::$ID_SIGN];
        $statusStr = $jsonArray[self::$STATUS_SIGN];
        global $dataStateMap;
        $dataState = $dataStateMap[$statusStr];
        $aoiId = $jsonArray[self::$AOIID_SIGN];
        return new DataDetail($dataType, $aoiId, $dataState, $time, $id, null);
    }

    static function parseOrderStatusJson($jsonArray){
        $typeStr = $jsonArray[self::$TYPE_SIGN];
        global $dataTypeMap;
        $dataType = $dataTypeMap[$typeStr];
        $time = $jsonArray[self::$TIMESTAMP_SIGN];
        $id = $jsonArray[self::$ID_SIGN];
        $statusStr = $jsonArray[self::$STATUS_SIGN];
        global $dataStateMap;
        $dataState = $dataStateMap[$statusStr];
        $aoiId = $jsonArray[self::$AOIID_SIGN];
        $expectTime=null;
        if($dataState=="orderadd"){
            $expectTime = $jsonArray["expect_time"];
        }
        return new DataDetail($dataType, $aoiId, $dataState, $time, $id, $expectTime);
    }

}