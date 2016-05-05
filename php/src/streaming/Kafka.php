<?php
$temp = __DIR__;
$spark_php_home = substr($temp,0,strrpos($temp,"/")-3);
require($spark_php_home."/src/storagelevel.php");

class KafkaUtils{



    static function createStream($ssc, $zkQuorum, $groupId, $topics, $kafkaParams=null,
    $storageLevel=null, $keyDecoder=null, $valueDecoder=null){

        if($kafkaParams==null) {
            $kafkaParams = array();
        }
        $kafkaParams["zookeeper.connect"]=$zkQuorum;
        $kafkaParams["group.id"]=$groupId;
        $kafkaParams["zookeeper.connection.timeout.ms"]="20000";

        if(!is_array($topics)) {
            throw new Exception("topics should be dict");
        }

        if($storageLevel==null){

            StorageLevel::$DISK_ONLY =new StorageLevel(True, False, False, False);
            StorageLevel::$DISK_ONLY_2 =new StorageLevel(True, False, False, False, 2);
            StorageLevel::$MEMORY_ONLY =new StorageLevel(False, True, False, True);
            StorageLevel::$MEMORY_ONLY_2 =new StorageLevel(False, True, False, True, 2);
            StorageLevel::$MEMORY_ONLY_SER =new StorageLevel(False, True, False, False);
            StorageLevel::$MEMORY_ONLY_SER_2 =new StorageLevel(False, True, False, False, 2);
            StorageLevel::$MEMORY_AND_DISK =new StorageLevel(True, True, False, True);
            StorageLevel::$MEMORY_AND_DISK_2 =new StorageLevel(True, True, False, True, 2);
            StorageLevel::$MEMORY_AND_DISK_SER =new StorageLevel(True, True, False, False);
            StorageLevel::$MEMORY_AND_DISK_SER_2 =new StorageLevel(True, True, False, False, 2);
            StorageLevel::$OFF_HEAP =new StorageLevel(False, False, True, False, 1);

            $storageLevel=StorageLevel::$MEMORY_AND_DISK_SER_2;
        }

        $jlevel = $ssc->sc->getJavaStorageLevel($storageLevel);

        $helperClass = $ssc->php_call_java->newThread()->currentThread()->getContextClassLoader()->loadClass("org.apache.spark.streaming.kafka.KafkaUtilsPythonHelper");
        $helper = $helperClass->newInstance();
        $jstream = $helper->createStream($ssc->jssc, $kafkaParams, $topics, $jlevel);
        $stream = new dstream($jstream, $ssc, new utf8_deserializer());
        return $stream;
    /*    return $stream->map(
            function($k_v){
                file_put_contents("/home/".get_current_user()."/php_worker10.txt", $k_v."!!!\n", FILE_APPEND);
                return array($k_v[0],$k_v[1]);
            });
    */
    }






}