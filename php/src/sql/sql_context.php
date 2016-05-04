<?php
class sql_context{


    function __construct($sparkContext,$sqlContext=null){
        $this->sc = $sparkContext;
        $this->jsc = $this->sc->jsc;
        $this->php_call_java = $this->sc->php_call_java;
        $this->scala_SQLContext = $sqlContext;


    }

    function createDataFrame($data,$schema,$samplingRatio=null){

        if($data instanceof DataFrame){
            throw new Exception("data is already a DataFrame");
        }
        $rdd=null;
        $schema=null;
        if($data instanceof rdd) {
            $temp = $this->createFromRDD($data, $schema, $samplingRatio);
            $rdd = $temp[0];
            $schema = $temp[1];
        }else{
            $temp = $this->createFromLocal($data, $schema);
            $rdd = $temp[0];
            $schema = $temp[1];
        }
        $jrdd = $this->php_call_java->SerDeUtil->toJavaArray($rdd->to_java_object_rdd());
        $jdf = $this->ssql_ctx->applySchemaToPhpRDD($jrdd->rdd(), $schema->json());
        $df = new DataFrame($jdf, $this);
        $df->schema = $schema;
        return $df;
    }

    function createFromRDD($rdd,$schema,$samplingRatio){
        if($schema==null or is_array($schema)){
            $struct = $this->inferSchema($rdd,$samplingRatio);
            $converter = create_converter($struct);
            $rdd = $rdd->map($converter);
            $schema = $struct;
        }
        $rdd = $rdd->map($schema->toInternal);
        return array($rdd, $schema);
    }

    function inferSchema($rdd,$samplingRatio=null){
        $first = $rdd ->first();
        if($first == null){
            throw new Exception("The first row in RDD is empty, can not infer schema");
        }
    }

}