<?php

class rdd_sampler_base{

    var $withReplacement;
    var $seed;
    var $split;
    function __construct($withReplacement, $seed=null){
        if($seed != null) {
            $this->seed = $seed;
        }elseif ($this->seed==null){
            $this->seed = rand(0,getrandmax());
        }
        $this->withReplacement = $withReplacement;
    }

    function getPoissonSample($mean){
        if($mean < 20.0){
            $l = exp(-$mean);
            $p = rand($this->seed ^ $this->split);
            $k = 0;
            while($p>$l){
                $k++;
                $p*=rand($this->seed ^ $this->split);
            }
            return $k;
        }else{
            $p= $this->randomExponential($mean);
            $k=0;
            while($p<1.0){
                $k++;
                $p+=$this->randomExponential($mean);
            }
            return $k;
        }
    }

    function randomExponential($lambda){
        $pV=0;
        while(True){
            $pV = (float)(rand(0,getrandmax())/getrandmax());
            if($pV!=1){
                break;
            }
        }
        return (-1.0/$lambda)*log(1-$pV);
    }

}


class rdd_sampler extends rdd_sampler_base{
    var $fraction;
    function __construct($withReplacement,$fraction,$seed=null){
        parent::__construct($withReplacement,$seed);
        $this->fraction = $fraction;
    }

    function func($split,$iterator){
        $this->split = $split;
        if($this->withReplacement==True){
            foreach($iterator as $obj){
                $count = $this->getPoissonSample($this->fraction);
                for($i =0 ;$i<$count;$i++){
                    yield $obj;
                }
            }
        }else{
            foreach($iterator as $obj){
                $temp = (float)(rand(0,$this->seed ^ $this->split)/$this->seed ^ $this->split);
                if($temp<$this->fraction){
                    yield $obj;
                }
            }
        }
    }
}