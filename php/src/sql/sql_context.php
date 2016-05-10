<?php

require __DIR__."/types.php";
require __DIR__."/dataframe.php";

class sql_context{


    function __construct($sparkContext,$sqlContext=null){
        $this->sc = $sparkContext;
        $this->jsc = $this->sc->jsc;
        $this->php_call_java = $this->sc->php_call_java;
        $this->scala_SQLContext = $sqlContext;

        if($this->scala_SQLContext==null) {
            $this->scala_SQLContext = $this->php_call_java->newSQLContext($this->jsc->sc());
        }
        $this->ssql_ctx=$this->scala_SQLContext;
    }

    function createDataFrame($data,$schema,$samplingRatio=null){

        if($data instanceof DataFrame){
            throw new Exception("data is already a DataFrame");
        }
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
        $rdd = $rdd->map(
            function($obj) use ($schema) {
                return $schema->toInternal($obj);
            }
        );
        return array($rdd, $schema);
    }

    function inferSchema($rdd,$samplingRatio=null){
        $first = $rdd -> first();
        if($first == null){
            throw new Exception("The first row in RDD is empty, can not infer schema");
        }

        global $_infer_schema;
        global $_merge_type;
        if($samplingRatio==null) {

            $schema = $_infer_schema($first);

            if(_has_nulltype($schema)){
                $temp = $rdd->take(100);
                for($i=1;$i<100;$i++) {
                    $schema = $_merge_type($schema, $_infer_schema($temp[$i]));
                    if(!_has_nulltype($schema)) {
                        break;
                    }
                }
            }

        }else {
            if($samplingRatio < 0.99){
                $rdd = $rdd->sample(False, floatval($samplingRatio));
            }

            $schema = $rdd->map($_infer_schema)->reduce($_merge_type);
        }
        return $schema;
    }

    function createFromLocal($data,$schema){


    }

}