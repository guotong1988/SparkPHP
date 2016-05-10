<?php
class DataFrame{

    function __construct($jdf, $sql_ctx){
        $this->jdf = $jdf;
        $this->sql_ctx = $sql_ctx;
        $this->sc = $sql_ctx->sc;
        $this->is_cached = False;
        $this->schema = null;  # initialized lazily
        $this->lazy_rdd = null;

    }


    function printSchema()
    {
        print_r($this->jdf->schema()->treeString());
    }

}