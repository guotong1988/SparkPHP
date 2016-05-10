<?php



$ORDER_TYPE = "REALTIME_ORDER_STATUS";
$ORDER_TYPE_DUBIOUS = "REALTIME_DUBIOUS_ORDER";
$RIDER_TYPE = "REALTIME_RIDER_STATUS";
$RIDER_TYPE_POSITION = "REALTIME_RIDER_POSITION";
$ORDER_TYPE_PAYMENT = "REALTIME_ORDER_OVERTIMEPAYMENT";
$ORDER_TYPE_ILLEGAL = "REALTIME_ORDER_ILLEGAL";

$ORDER_ADD = "6";
$ORDER_DISTR = "20";
$ORDER_UNTAKE = "7";
$ORDER_TAKE = "8";
$ORDER_FINISH = "9";
$ORDER_CANCEL = "10";

$RIDER_POST = "1";
$RIDER_BREAK1 = "2";
$RIDER_BREAK2 = "3";
$RIDER_BREAK3 = "4";
$RIDER_BREAK4 = "5";
$RIDER_DROPPED = "0";

$dataTypeMap = array();

$dataTypeMap[$ORDER_TYPE]="order";
$dataTypeMap[$RIDER_TYPE]="rider";
$dataTypeMap[$ORDER_TYPE_DUBIOUS]="order dubious";
$dataTypeMap[$RIDER_TYPE_POSITION]="rider position";
$dataTypeMap[$ORDER_TYPE_PAYMENT]="order payment";
$dataTypeMap[$ORDER_TYPE_ILLEGAL]="order illegal";


$dataStateMap = array();

$dataStateMap[$ORDER_ADD] = "orderadd";
$dataStateMap[$ORDER_DISTR] =  "orderdistr";
$dataStateMap[$ORDER_UNTAKE] = "orderuntake";
$dataStateMap[$ORDER_TAKE]= "ordertake";
$dataStateMap[$ORDER_FINISH]= "orderfinish";
$dataStateMap[$ORDER_CANCEL]= "ordercancel";

$dataStateMap[$RIDER_POST]="上岗";
$dataStateMap[$RIDER_BREAK1]= "小休";
$dataStateMap[$RIDER_BREAK2]= "小休";
$dataStateMap[$RIDER_BREAK3]= "小休";
$dataStateMap[$RIDER_BREAK4]= "小休";
$dataStateMap[$RIDER_DROPPED]= "离岗";

