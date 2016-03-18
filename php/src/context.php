<?php
$temp = __FILE__;
$spark_php_home = substr($temp,0,strrpos($temp,"/")-3);
require($spark_php_home . "src/conf.php");
require($spark_php_home . "src/php_call_java.php");
require($spark_php_home . "src/rdd.php");
require($spark_php_home . "src/serializer.php");
class context {

    var $php_call_java;#就是php_call_java
    var $jsc;#JavaSparkContext
    var $conf;
    var $pickled_broadcast_vars;
    var $environment;
    var $python_includes;
    var $batch_size;
    var $serializer;
    var $unbatched_serializer;
    var $master;
    var $app_name;
    var $spark_home;
    var $php_exec;
    var $php_ver;
    var $java_accumulator;

    function context( $master=null, $app_name=null, $spark_home=null, $phpFiles=null,
                      $environment=null, $batchSize=0, $serializer=null, $conf=null,
                      $gateway=null, $jsc=null, $profiler_cls=null){
        echo "context构造方法（开始）";
        $this->ensure_initialized();
        $this->do_init($master, $app_name, $spark_home, $phpFiles, $environment, $batchSize, $serializer,
            $conf, $jsc, $profiler_cls);
        echo "context构造方法（结束）";
    }

    function do_init($master, $app_name, $spark_home, $phpFiles, $environment, $batch_size, $serializer,
                 $conf, $jsc, $profiler_cls){
        if($environment==null) {
            $this->environment = array();
        }else{
            $this->environment = $environment;
        }
        $this->conf =new conf(null,$this);
        $this->$batch_size = $batch_size;
        $this->unbatched_serializer = $serializer;
        if($batch_size == 0) {
            $this->serializer = new auto_batched_serializer($this->unbatched_serializer);
        }else {
            $this->serializer = new batched_serializer($this->unbatched_serializer, $batch_size);
        }

        if($master) {
            $this->conf->set_master($master);
        }
        if($app_name) {
            $this->conf->set_app_name($app_name);
        }

        if($spark_home) {
            $this->conf->set_spark_home($spark_home);
        }

        if($environment) {
            foreach ($environment as $key => $value){
                $this->conf->set_executor_env($key, $value);
            }
        }

        $DEFAULT_CONFIGS = array(
            "spark.serializer.objectStreamReset" => 100,
             "spark.rdd.compress"=> True
        );

        foreach($DEFAULT_CONFIGS as $key=>$value) {
            $this->conf->set_if_missing($key, $value);
        }

        if(!$this->conf->contains("spark.master")){
            throw new Exception("A master URL must be set in your configuration");
        }
        if(!$this->conf->contains("spark.app.name")) {
            throw new Exception("An application name must be set in your configuration");
        }
        # Read back our properties from the conf in case we loaded some of them from
        # the classpath or an external config file
        $this->master = $this->conf->get("spark.master");
        $this->app_name = $this->conf->get("spark.app.name");
        $this->spark_home = $this->conf->get("spark.home", null);

        # Let YARN know it's a pyspark app, so it distributes needed libraries.
        if($this->master == "yarn-client"){
            $this->conf->set("spark.yarn.isPython", "true");
        }

      #  for (k, v) in self._conf.getAll():
      #      if k.startswith("spark.executorEnv."):
      #          varName = k[len("spark.executorEnv."):]
      #          self.environment[varName] = v
      #  if sys.version >= '3.3' and 'PYTHONHASHSEED' not in os.environ:
            # disable randomness of hash of string in worker, if this is not
            # launched by spark-submit
      #      self.environment["PYTHONHASHSEED"] = "0"


        $this->jsc = $this->initialize_context($this->conf->jconf);



        # Create a single Accumulator in Java that we'll send all our updates through;
        # they will be passed back to us through a TCP server
     #TODO   $accumulators =new AccumulatorServer();
     #TODO   $this->accumulator_server = $accumulators->_start_update_server();
     #TODO   $temp = $this->accumulator_server->server_address;
        $host ='127.0.0.1';#TODO
        $port =0;#TODO
        $this->java_accumulator = $this->jsc->accumulator(
                $this->php_call_java->new_java_list(),
                $this->php_call_java->php_accumulator_param($host, $port));

        $this->php_exec = "php";
        $this->php_ver = "5.6";#TODO



        $this->pickled_broadcast_vars = array();
        $this->python_includes = array();
    }

    function ensure_initialized(){
        #TODO synchronized
        if($this->php_call_java==null) {
            $this->php_call_java = new php_call_java();
        }
    }

    function initialize_context($jconf){
        $this->php_call_java->JavaSparkContext->set($jconf);
        return $this->php_call_java->JavaSparkContext;
    }

    function parallelize($data, $numSlices){
        if(is_array($data)){
        }
    }

    function text_file($filePath,$minPartitions=null,$use_unicode=True){
        if ($minPartitions==null){
            $minPartitions=1;
        }
        $HadoopRDD = $this->jsc->textFile($filePath, $minPartitions);
        $serializer = new utf8_deserializer($use_unicode);
        #echo $HadoopRDD->rdd();#就是取RDD类
        return new rdd($HadoopRDD, $this, $serializer);
    }


}

