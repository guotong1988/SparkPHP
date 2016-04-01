<?php



class serializer {
    function dump_stream($iterator, $stream){}
    function load_stream($stream){
        return array();
    }


    function is_list($arr){
        return is_array($arr) && ($arr == array() || array_keys($arr) === range(0,count($arr)-1) );
    }

    #special_lengths
    var $END_OF_DATA_SECTION = -1;
    var $PHP_EXCEPTION_THROWN = -2;
    var $TIMING_DATA = -3;
    var $END_OF_STREAM = -4;
    var $NULL = -5;
}



class utf8_serializer extends serializer{
    var $use_unicode;
    function utf8_serializer($use_unicode=True){
        $this->use_unicode = $use_unicode;
    }

    function dump_stream($iterator, sock_output_stream $stream){
        if($this->is_list($iterator)) {
            foreach ($iterator as $element) {
                if (is_array($element)) {#pair等元组的情况
                    $stream->write_utf2(serialize($element));
                } else {
                    $stream->write_utf2($element);
                }
            }
        }else{#进来的就是key是string的pair等元组的情况
            $index=0;
            $newArray = array();
            foreach($iterator as $key=>$element){
                $temp=array();
                array_push($temp,$key);
                array_push($temp,$element);
                $newArray[$index]=$temp;
                $index++;
            }
            foreach ($newArray as $element) {
                if (is_array($element)) {#已经转化成元组
                    $stream->write_utf2(serialize($element));
                } else {
                    $stream->write_utf2($element);
                }
            }
        }
    }


    function dump_stream4file($iterator, file_output_stream $stream){#TODO 模仿上面
        foreach($iterator as $key=>$element)
        {
            if(is_string($key)){
                $stream->write_utf2($key);
                $stream->write_utf2($element);
            }else{
                if(is_string($element)) {
                    $stream->write_utf2($element);
                }elseif(is_array($element)){#pair情况
                    $stream->write_utf2(serialize($element));
                }else{#integer情况
                    $stream->write_utf2($element);
                }
            }
        }
    }

}


class utf8_deserializer extends serializer{
    var $use_unicode;
    var $is_array=False;

    function utf8_deserializer($use_unicode=True){
        $this->use_unicode=$use_unicode;
    }


    function loads(sock_input_stream $stream)
    {
        $length_of_line = $stream->read_int();
        if($length_of_line == 4294967295){#TODO -1
            throw new Exception("end of data");
        }elseif($length_of_line == $this->NULL) {
            return null;
        }
        $string = $stream->read_fully($length_of_line);

        file_put_contents("/home/gt/php_worker18.txt", "here ".$string."\n\n\n\n", FILE_APPEND);

        if($this->is_array==False) {
            if (is_array(unserialize($string))) {
                $this->is_array = True;
            }
        }
        if($this->is_array==False){
            if ($this->use_unicode==True){
                return $string;
            }else{
                return $string;
            }
        }else{#pair的情况
            return unserialize($string);
        }
    }


    function load_stream($stream)
    {
        $item_array= array();
        try {
            while(True){
                $temp2 = $this->loads($stream);
                if($temp2!="") {
                    array_push($item_array, $temp2);
                }
            }
        }catch (Exception $e){
            return $item_array;
        }
    }
}



class batched_serializer extends serializer
{
    #  Serializes a stream of objects in batches by calling its wrapped
    #  Serializer with streams of objects.
    var $UNLIMITED_BATCH_SIZE = -1;
    var $UNKNOWN_BATCH_SIZE = 0;
    var $serializer;
    var $batch_size;

    function __construct($serializer, $batch_size = UNLIMITED_BATCH_SIZE)
    {
        $this->serializer = $serializer;
        $this->batch_size = $batch_size;
    }

    function batched($iterator)
    {
        if ($this->batch_size == $this->UNLIMITED_BATCH_SIZE) {
            return $iterator;
        }
        #TODO
    }

    function dump_stream($iterator, $stream)
    {
        $this->serializer->dump_stream($this->batched($iterator), $stream);
    }

    function load_stream($stream)
    {
        #TODO
    }

    function _load_stream_without_unbatching($stream)
    {
        return $this->serializer->load_stream($stream);
    }

}



class auto_batched_serializer extends batched_serializer
{
    #   Choose the size of batch automatically based on the size of object
    var $best_size;

    function __construct($serializer, $best_size = 1 << 16)
    {
        parent::__construct($serializer, $this->UNKNOWN_BATCH_SIZE);
        $this->best_size = $best_size;
    }

    function dump_stream($iterator, $stream)
    {
        $batch = 1;
        $best = $this->best_size;
        while (True) {
            #TODO
        }
    }

}