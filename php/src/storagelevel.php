<?php

class StorageLevel{

    var $useDisk;
    var $useMemory;
    var $useOffHeap;
    var $deserialized;
    var $replication;

    function __construct($useDisk, $useMemory, $useOffHeap, $deserialized, $replication=1)
    {
        $this->useDisk = $useDisk;
        $this->useMemory = $useMemory;
        $this->useOffHeap = $useOffHeap;
        $this->deserialized = $deserialized;
        $this->replication = $replication;
    }

    static $DISK_ONLY;
    static $DISK_ONLY_2;
    static $MEMORY_ONLY;
    static $MEMORY_ONLY_2;
    static $MEMORY_ONLY_SER;
    static $MEMORY_ONLY_SER_2;
    static $MEMORY_AND_DISK;
    static $MEMORY_AND_DISK_2;
    static $MEMORY_AND_DISK_SER;
    static $MEMORY_AND_DISK_SER_2;
    static $OFF_HEAP;
}

