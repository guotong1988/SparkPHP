<?php


class accumulator
{
    static $accumulatorRegistry = array();

    var $aid;
    var $value;
    var $accum_param;
    var $deserialized;

    function deserialize_accumulator($aid, $zero_value, $accum_param){
        $accum =new accumulator($aid, $zero_value, $accum_param);
        $accum->deserialized = True;
        self::$accumulatorRegistry[$aid] = $accum;
        return $accum;
    }


    function __construct($aid=null, $value=null, $accum_param=null)
    {
        $this->aid = $aid;
        $this->accum_param = $accum_param;
        $this->value = $value;
        $this->deserialized = False;
        self::$accumulatorRegistry[$aid] = $this;
    }

    function add($term){
        $this->value = $this->accum_param->addInPlace($this->value, $term);
    }

}


class AccumulatorParam
{

    static function zero($value){

    }

    static function addInPlace($value1, $value2){

    }

}




class AddingAccumulatorParam extends AccumulatorParam
{
    static  $zero_value;

    function __construct($zero_value)
    {
        $this->zero_value = $zero_value;
    }

    static function zero($value)
    {
        return self::$zero_value;
    }

    static function addInPlace($value1, $value2)
    {
        $value1 += $value2;
        return $value1;
    }
}



class AccumulatorServer{

    static $server_shutdown = False;
    static $pid=-1;
    function shutdown()
    {
        echo "AccumulatorServer关闭";
        self::$server_shutdown = True;
        print("!!!!!!!!!!!!!!!!!!!".self::$pid);
        print(posix_kill(self::$pid, SIGKILL));
    }


    function start_update_server(){
        echo "accumulator start_update_server()开始\n";

        $address = '127.0.0.1';
        $port = 18083;

        $message_queue_key = ftok(__FILE__, 'a');
        $message_queue = msg_get_queue($message_queue_key, 0666);

        $nPID = pcntl_fork(); // 创建子进程
        if ($nPID == 0) {// 子进程过程
            //创建端口
            if( ($sock = socket_create(AF_INET, SOCK_STREAM, SOL_TCP)) === false) {
                echo "socket_create() failed :reason:" . socket_strerror(socket_last_error()) . "\n";
            }
            //绑定
            $bind = socket_bind($sock, $address, $port);
            if ($bind === false) {
                $port= $port + 1;
                $bind = socket_bind($sock, $address, $port);
                if ($bind === false) {
                    echo "accumulator socket_bind() 失败:" . socket_strerror(socket_last_error($sock)) . "\n";
                }
            }

            if( ( $ret = socket_listen( $sock, 0 ) ) < 0 )
            {
                echo "failed to listen to socket: ".socket_strerror($ret)."\n";
                exit();
            }

            msg_send($message_queue, 1, getmypid());

            socket_set_nonblock($sock);
            while(!self::$server_shutdown) {
                //得到一个链接
                    $temp = socket_accept($sock);
                    if($temp != False) {
                        #file_put_contents("/home/gt/php_worker.txt", "accumulator socket_accept()成功\n",FILE_APPEND);
                        $sis = new sock_input_stream($temp);
                        $num_updates = $sis->read_int();
                        echo $num_updates;
                        for ($i = 0; $i < $num_updates; $i++) {
                            $tempArray = $sis->read_fully($sis->read_int());
                            echo $tempArray;
                            accumulator::$accumulatorRegistry[$tempArray[0]] = $tempArray[1];
                        }
                    #    $sos = new sock_output_stream($temp);
                    #    $sos->write_int(1);
                    #    echo "$$$$$$$$";
                    #    socket_close($temp);
                    #    echo "%%%%%%%%";
                    }else{
                        echo self::$server_shutdown;
                     #   sleep(1);
                    }
            } ;

            echo "accumulator sock subprocess关闭\n";
            socket_close($sock);

        }

        msg_receive($message_queue, 0, $message_type, 1024, $message, true, MSG_IPC_NOWAIT);
        self::$pid = $message;
        return array($address,$port);
    }
}
