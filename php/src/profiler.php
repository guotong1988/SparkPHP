<?php

class ProfilerCollector{

    var $profiler_cls;


    function __construct($profiler_cls,$dump_path){

        $this->profiler_cls = $profiler_cls;
        $this->profile_dump_path = $dump_path;
        $this->profilers = array();

    }

}

class Profiler{

}



class PStatsParam extends AccumulatorParam{
   # """PStatsParam is used to merge pstats.Stats"""

    static function zero($value)
    {
        return null;
    }

    static function addInPlace($value1, $value2)
    {
        if($value1==null){
            return $value2;
        }
        return $value1 + $value2;
    }
}


class BasicProfiler extends Profiler{

    function __construct($ctx){

        $temp = new PStatsParam();
        # Creates a new accumulator for combining the profiles of different
        # partitions of a stage
        $this->accumulator = $ctx->accumulator(null, $temp);

    }


    function profile($func)
    {
    # Adds a new profile to the existing accumulated value
        $this->accumulator->add(1);
    }

    function stats(){
        $this->accumulator->value;
    }

}