<?php

class sock_input_stream {
    private $sock = null;

    public function __construct( $sock ){
        $this->sock = $sock;
    }

    public function __destruct(){
    }

    /**
     * 读取字节
     *
     * @param $len 想要读的字节数
     * @return string
     */
    private function read( $len=1 ){
        $len = intval( $len );
        if( $len>0 ) {
            $read = socket_read($this->sock,$len);
            if( strlen($read)!=$len ){
                throw new Exception('流文件已经到末尾，读取错误',222);
            } else {
                return $read;
            }
        } else {
            return '';
        }
    }

    /**
     * 读取一个字节,并转换成int
     *
     * @return int
     */
    public function read_byte(){
        return hexdec(bin2hex($this->read(1)));
    }

    /**
     * 短整形
     *
     * @return int
     */
    public function read_short(){
        return hexdec(bin2hex($this->read(2)));
    }

    /**
     * 整形数据
     *
     * @return int
     */
    public function read_int(){
        return hexdec(bin2hex($this->read(4)));
    }

    /**
     * 读取一个字符串
     *
     * @return string
     */
    public function read_utf(){
        $len = $this->readInt();
        return $this->read($len);
    }

    /**
     * 读取指定长度的数据,并作为字符串返回,图片等二进制文件,可能需要这样读取.
     *
     * @param int $len
     */
    public function read_fully( $len ) {
        return $this->read( $len );
    }

    /**
     * readUTF的别名
     *
     */
    public function read_string(){
        return $this->readUTF();
    }

    /**
     * 长整形.
     *
     */
    public function read_long(){
        return hexdec(bin2hex($this->read(8)));
    }
}