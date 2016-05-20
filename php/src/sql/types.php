<?php


function need_converter($dataType)
{

    if($dataType instanceof StructType) {
        return True;
    }
    elseif($dataType instanceof ArrayType) {
        return need_converter($dataType->elementType);
    }
    elseif($dataType instanceof MapType){
        need_converter($dataType->keyType) or need_converter($dataType->valueType);
    }
    elseif($dataType instanceof NullType) {
        return True;
    } else {
        return False;#也就是说不是Array的话就不用转
    }
}

function create_converter($dataType){
    if  (!need_converter($dataType)) {
        return function($x){return $x;};
    }
    if ($dataType instanceof ArrayType) {
        $conv = create_converter($dataType->elementType);
        return function ($row) use ($conv) {
            $re = array();
            foreach ($row as $field) {
                array_push($re, $conv($field));
            }
            return $re;
        };
    }elseif($dataType instanceof MapType){
        $kconv = create_converter($dataType->keyType);
        $vconv = create_converter($dataType->valueType);
        return function($row) use ($kconv,$vconv){
            $re = array();
            foreach($row as $key=>$value){
                $re[$kconv($key)]=$vconv($value);
            }
            return $re;
        };
    }elseif($dataType instanceof NullType) {
        return function($x){return null;};
    }
    elseif(!($dataType instanceof StructType)){
        return function($x){return $x;};
    }


    $names = array();
    foreach($dataType->fields as $f){
        array_push($names,$f->name);
    }
    $converters = array();
    foreach($dataType->fields as $f){
        array_push($converters,create_converter($f->dataType));
    }
    $convert_fields = True;
    foreach($dataType->fields as $f){
        if(need_converter($f->dataType)==False){
            $convert_fields=False;
        };
    }

    $convert_struct = function ($obj) use ($convert_fields,$converters)
    {
        if($obj==null) {
            return;
        }

        if(is_array($obj)) {
            if($convert_fields) {
                $re = array();
                for($i=0;$i<sizeof($obj);$i++){
                    array_push($re,$converters[$i]($obj[$i]));
                }
                return $re;
            }else{
                return $obj;
            }
        }

    };
    return $convert_struct;
}




function _infer_type ($obj){
    if($obj==null) {
        return new NullType();
    }
    $dataType = null;
    if(is_bool($obj)){
        $dataType = new BooleanType();
    }elseif(is_int($obj)){
        $dataType = new LongType();
    }elseif(is_float($obj)){
        $dataType= new DoubleType();
    }elseif(is_string($obj)){
        $dataType = new StringType();
    }
    if($dataType!=null){
        return $dataType;
    }

    if(is_array($obj)){          #array全部对应为MapType
        foreach($obj as $k=>$v){
            return new MapType(_infer_type($k),_infer_type($v),True);
        }
    }else{
        global $_infer_schema;
        return $_infer_schema($obj);
    }
}

$_infer_schema = function ($row){

    $fields = array();
    foreach($row as $k=>$v){
        array_push($fields,new StructField($k,_infer_type($v),True));
    }
    return new StructType($fields);

};

$_merge_type = function ($a,$b){
    if($a instanceof NullType) {
        return $b;
    }elseif ($b instanceof NullType){
        return $a;
    }
};


function _has_nulltype($dt)
{
    if ($dt instanceof StructType) {
        foreach($dt->fields as $e){
            if (_has_nulltype($e->dataType)){
                 return true;
            }
        }
        return false;
    } elseif ($dt instanceof ArrayType) {
        return _has_nulltype($dt->elementType);
    } else {
        return $dt instanceof NullType;
    }
}


class DataType{

    function typeName(){
        return strtolower(substr(__CLASS__,0,sizeof(__CLASS__)-5));#为了match DataType.scala里的130行
    }

    function jsonValue()
    {
        return $this->typeName();
    }

    function json()
    {
        return json_encode($this->jsonValue());
    }

    function toInternal($obj)
    {
    #        Converts a Python object into an internal SQL object.
        return $obj;
    }

    function fromInternal($obj)
    {
    #        Converts an internal SQL object into a native Python object.
        return $obj;
    }

    function needConversion()
    {
    #        Does this type need to conversion between Python object and internal SQL object.
    #        This is used to avoid the unnecessary conversion for ArrayType/MapType/StructType.
        return False;
    }
}


class FractionalType extends NumericType{

}

class DoubleType extends FractionalType{

    function typeName(){
        return strtolower(substr(__CLASS__,0,sizeof(__CLASS__)-5));#为了match DataType.scala里的130行
    }

    function jsonValue()
    {
        return $this->typeName();
    }

}

class NullType extends DataType{

    function typeName(){
        return strtolower(substr(__CLASS__,0,sizeof(__CLASS__)-5));#为了match DataType.scala里的130行
    }

    function jsonValue()
    {
        return $this->typeName();
    }

}

class AtomicType extends  DataType{

}

class BooleanType extends AtomicType{

    function typeName(){
        return strtolower(substr(__CLASS__,0,sizeof(__CLASS__)-5));#为了match DataType.scala里的130行
    }

    function jsonValue()
    {
        return $this->typeName();
    }

}

class IntegralType extends NumericType{

    function typeName(){
        return strtolower(substr(__CLASS__,0,sizeof(__CLASS__)-5));#为了match DataType.scala里的130行
    }

    function jsonValue()
    {
        return $this->typeName();
    }

}

class NumericType extends AtomicType{

}

class StringType extends AtomicType{

    function typeName(){
        return strtolower(substr(__CLASS__,0,sizeof(__CLASS__)-5));#为了match DataType.scala里的130行
    }

    function jsonValue()
    {
        return $this->typeName();
    }

}

class IntegerType extends IntegralType
{

    function typeName(){
        return strtolower(substr(__CLASS__,0,sizeof(__CLASS__)-5));#为了match DataType.scala里的130行
    }

    function jsonValue()
    {
        return $this->typeName();
    }

    function simpleString()
    {
    return 'int';
    }
}

class LongType extends IntegralType{

}


class StructType extends DataType{
    var $fields;
    var $names;
    var $needSerializeAnyField;
    function __construct($fields=null)
    {
        if($fields==null) {
            $this->fields = array();
            $this->names = array();
        }
        else {
            $this->fields = $fields;
            $this->names= array();
            foreach($fields as $e){
                array_push($this->names,$e);
            }
        }

        $this->needSerializeAnyField = True;
        foreach($this->fields as $f){
            if($f->needConversion==False){
                $this->needSerializeAnyField=False;
            }
        }
    }


    function typeName(){
        return strtolower(substr(__CLASS__,0,sizeof(__CLASS__)-5));#为了match DataType.scala里的130行
    }


    #override
    function jsonValue()
    {
        $temp = array();
        foreach($this->fields as $f){
            array_push($temp,$f->jsonValue());
        }

        return array(
            "fields" =>  $temp,
            "type"=> $this->typeName(),
       );
    }

    function toInternal($obj)
    {
        if($obj==null){
            return;
        }
        if(is_string($obj)){
            $obj = explode(" ",$obj);
            return $obj;
        }

        if($this->needSerializeAnyField) {
            if(is_array($obj)) {
                $re = array();
                for($i=0;$i<sizeof($this->fields);$i++){
                    array_push($re,$this->fields[$i]->toInternal($obj[$i]));
                }
                return $re;
            } else {
                    throw new Exception("Unexpected tuple %r with StructType",$obj);
            }
        }else{
            if(is_array($obj)) {
                return array($obj);
//                $re = array();
//                for($i=0;$i<sizeof($this->names);$i++){
//                    array_push($re,$obj[$i]);
//                }
//                return $re;
            }else{
                throw new Exception("Unexpected tuple %r with StructType",$obj);
            }
        }
    }

    function needConversion()
    {
        return True;
    }
}


class ArrayType extends DataType{

    var $elementType;
    var $containsNull;

    function __construct($elementType, $containsNull=True)
    {
        $this->elementType = $elementType;
        $this->containsNull = $containsNull;
    }
}

class StructField extends DataType{

    function __construct($name, $dataType, $nullable=True, $metadata=null){
        $this->name = $name;
        $this->dataType = $dataType;
        $this->nullable = $nullable;
        $this->metadata = $metadata;
    }

    function needConversion()
    {
        return $this->dataType->needConversion();
    }

    function toInternal($obj)
    {
        return $this->dataType->toInternal($obj);
    }

    #override
    function jsonValue()
    {
        return array(
        #    "metadata" => $this->metadata,
            "name" => $this->name,
            "nullable" => $this->nullable,
            "type" => $this->dataType->jsonValue(),
        );
    }
}

class MapType extends DataType{
    var $keyType;
    var $valueType;
    var $valueContainsNull;
    function __construct($keyType, $valueType, $valueContainsNull=True)
    {
        $this->keyType = $keyType;
        $this->valueType = $valueType;
        $this->valueContainsNull = $valueContainsNull;
    }

    function needConversion(){
        return $this->keyType->needConversion() or $this->valueType->needConversion();
    }

    function typeName(){
        return strtolower(substr(__CLASS__,0,sizeof(__CLASS__)-5));#为了match DataType.scala里的130行
    }

    function jsonValue()
    {
        return $this->typeName();
    }

    function toInternal($obj)
    {
        if(!$this->needConversion()){
            return $obj;
        }
        $re = array();
        foreach($obj as $k=>$v){
            $re[$this->keyType->toInternal($k)]=$this->valueType->toInternal($v);
        }
        return $re;
    }
}