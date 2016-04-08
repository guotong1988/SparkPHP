<?php



class serializer {
    function dump_stream($iterator, $stream){}
    function load_stream($stream){
        return array();
    }


    function is_list($arr){
        return is_array($arr) && ($arr == array() || array_keys($arr) === range(0,count($arr)-1) );
    }

    function is_list4generator(Generator $g){
        if($g->key()==0){
                return True;
        }
        return False;
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

    function dump_stream($iterator,$stream){
        if($iterator instanceof Generator){
            if ($this->is_list4generator($iterator)) {
                foreach ($iterator as $element) {
                    if (is_array($element)) {#pair等元组的情况
                        $stream->write_utf2(serialize($element));
                    } else {
                        $stream->write_utf2($element);
                    }
                }
            }else{
                $index = 0;
                $newArray = array();
                foreach ($iterator as $key => $element) {
                    $temp = array();
                    array_push($temp, $key);
                    array_push($temp, $element);
                    $newArray[$index] = $temp;
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
        }else {
            if ($this->is_list($iterator)) {
                foreach ($iterator as $element) {
                    if (is_array($element)) {#pair等元组的情况
                        $stream->write_utf2(serialize($element));
                    } else {
                        $stream->write_utf2($element);
                    }
                }
            } else {#进来的就是key是string的pair等元组的情况
                $index = 0;
                $newArray = array();
                foreach ($iterator as $key => $element) {
                    $temp = array();
                    array_push($temp, $key);
                    array_push($temp, $element);
                    $newArray[$index] = $temp;
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
    }


    function dump_stream4file($iterator,$stream){
        $stream->write_utf(serialize($iterator));
  /*
        if($this->is_list($iterator)) {
            foreach ($iterator as $element) {
                if (is_array($element)) {#pair等元组的情况
                    $stream->write_utf(serialize($element));
                } else {
                    $stream->write_utf($element);
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
                    $stream->write_utf(serialize($element));
                } else {
                    $stream->write_utf($element);
                }
            }
        }*/
    }

}


class utf8_deserializer extends serializer{
    var $use_unicode;
    var $is_array=False;
    var $need_check=True;
    function utf8_deserializer($use_unicode=True){
        $this->use_unicode=$use_unicode;
    }


    function loads($stream)
    {

        $length_of_line = $stream->read_int();
        if($length_of_line == 4294967295){#TODO -1
            throw new Exception("end of data");
        }elseif($length_of_line == $this->NULL) {
            return null;
        }
        $string = $stream->read_fully($length_of_line);
#     TODO 很奇怪之前把这个注释打开就正确了，可能之前java有日志 写得太慢了
        usleep(1);
        if($this->is_array==False && $this->need_check==True) {
            if (is_array(unserialize($string))) {
                $this->is_array = True;
            }
            $this->need_check = False;
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
        try {
            while(True){
                $temp2 = $this->loads($stream);
                if($temp2!="") {
                    yield $temp2;
                }
            }
        }catch (Exception $e){
            $this->is_array = False;
            $this->need_check = True;
        }
    }

    function load_stream4file($stream)
    {
        try {
            while(True){
                $temp2 = $this->loads4file($stream);
                if($temp2!="") {
                    yield $temp2;
                }
            }
        }catch (Exception $e){
            $this->is_array = False;
            $this->need_check = True;
        }
    }

    function loads4file($stream)
    {
        $length_of_line = $stream->read_int();
        if($length_of_line == 4294967295){#TODO -1
            throw new Exception("end of data");
        }elseif($length_of_line == $this->NULL) {
            return null;
        }
        $string = $stream->read_fully($length_of_line);
        if($string == ""){
            throw new Exception("end of data");
        }
        if($this->is_array==False && $this->need_check==True) {
            if (is_array(unserialize($string))) {
                $this->is_array = True;
            }
            $this->need_check = False;
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