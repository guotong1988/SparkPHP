<?php




class serializers {
    function dump_stream($iterator, $stream){}
    function load_stream($stream){
        return array();
    }
    #special_lengths
    var $END_OF_DATA_SECTION = -1;
    var $PHP_EXCEPTION_THROWN = -2;
    var $TIMING_DATA = -3;
    var $END_OF_STREAM = -4;
    var $NULL = -5;

}

class utf8_deserializer extends serializers{
    var $use_unicode;
    function UTF8Deserializer($use_unicode=True){
        $this->use_unicode=$use_unicode;
    }

    function loads(sock_input_stream $stream)
    {
#        $length_of_line = $stream->readInt();
#        echo $length_of_line."!!!!\n";
#        if($length_of_line == $this->END_OF_DATA_SECTION){
#            throw new Exception();
#        }elseif($length_of_line == $this->NULL) {
#            return null;
#        }
        $string = $stream->readUTF();
        if ($this->use_unicode==True){
            return $string;
        }else{
            return $string;
        }
    }


    function load_stream($stream)
    {
        $item_array= array();
        try {
            for($i=0;$i<10;$i++){
#            while(True){
                array_push($item_array, $this->loads($stream));
            }
        }catch (Exception $e){
            return $item_array;
        }
    }
}