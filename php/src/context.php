<?php
$temp = __FILE__;
$spark_php_home = substr($temp,0,strrpos($temp,"/")-3);
require($spark_php_home . "src/conf.php");
require($spark_php_home . "src/php_call_java.php");
require($spark_php_home . "src/rdd.php");
require($spark_php_home . "src/serializers.php");
require($spark_php_home . "src/accumulators.php");
class context {

    var $php_call_java;#就是pyspark里的jvm
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
    var $accumulator_server;
    var $defaultParallelism;
    var $profiler_collector;
    var $next_accum_id = 0;

    function context( $master=null, $app_name=null, $spark_home=null, $phpFiles=null,
                      $environment=null, $batchSize=0, $serializer=null, $conf=null,
                      $gateway=null, $jsc=null, $profiler_cls=null){
        $this->ensure_initialized();
        $this->do_init($master, $app_name, $spark_home, $phpFiles, $environment, $batchSize, $serializer,
            $conf, $jsc, $profiler_cls);
        $this->defaultParallelism=$this->jsc->sc()->defaultParallelism();
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
            $this->serializer = new utf8_serializer();#TODO
        }else {
            $this->serializer = new utf8_serializer();
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
        $this->accumulator_server =new AccumulatorServer();
        $temp = $this->accumulator_server->start_update_server();
        $host =$temp[0];
        $port =$temp[1];


        $this->java_accumulator = $this->jsc->accumulator(
                $this->php_call_java->new_java_list(),
                $this->php_call_java->php_accumulator_param($host, $port));

        $this->php_exec = getenv("SPARKPHP_DRIVER_PHP");
        $this->php_ver = 7.0;


        $this->pickled_broadcast_vars = array();
        $this->python_includes = array();


        if($this->conf->get("spark.python.profile", "false") == "true"){
            $dump_path = $this->conf->get("spark.python.profile.dump");
            $this->profiler_collector = new ProfilerCollector($profiler_cls, $dump_path);
        }

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
        $my_file = fopen("/home/".get_current_user()."/php_master.txt", "w");
        $fos = new file_output_stream($my_file);
        for($i=0;$i<sizeof($data);$i++) {
            $fos->write_utf($data[$i]);
        }
        fclose($my_file);
        $jrdd = $this->php_call_java->PhpRDD->readRDDFromFile($this->jsc, "/home/gt/php_master.txt", $numSlices);
        unlink("/home/".get_current_user()."/php_master.txt");
        return new rdd($jrdd,$this,null);
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

    function accumulator($value,$accum_param=null){
        if ($accum_param==null){
            if(is_int($value)){
                $accum_param = new AddingAccumulatorParam(0);
            }elseif(is_float($value)){
                $accum_param = new AddingAccumulatorParam(0.0);
            }
        }
        $this->$next_accum_id += 1;
        return new accumulator($this->$next_accum_id - 1, $value, $accum_param);
    }


    function convert_map($php_map)
    {
        if($php_map==null){
            return $this->php_call_java->new_java_map();
        }

        $jmap = $this->php_call_java->new_java_map();

        foreach ($php_map as $key => $value) {
            echo $key;
            echo "!!!\n";
            echo $value;
            echo "!!!\n";
            $jmap->put($key,$value);
        }
        return $jmap;
    }

    function newAPIHadoopFile($path,$inputFormatClass,$keyClass,$valueClass,$keyConverter="",$valueConverter="",$conf=null,$batchSize=0){
        $jconf = $this->convert_map($conf);
        $jrdd = $this->php_call_java->PhpRDD->newAPIHadoopFile($this->jsc, $path, $inputFormatClass, $keyClass,
                $valueClass, $keyConverter, $valueConverter,
                $jconf, $batchSize);
        return new rdd($jrdd, $this,new utf8_deserializer());
    }


    function runJob($rdd,$partitionFunc,$partitions=null,$allowLocal=False){
        if($partitions==null) {
            $partitions = range(1,$rdd->jrdd->partitions()->size());
        }
        $mappedRDD = $rdd->mapPartitions($partitionFunc);
        $port = $this->php_call_java->PhpRDD->runJob($this->jsc->sc(), $mappedRDD->jrdd,$partitions);
        return $this->load_from_socket($port, $mappedRDD->jrdd_deserializer);#是个utf8_serializer
    }


    function load_from_socket($port, $deserializer)
    {
       # print_r($deserializer);
        $sock = socket_create(AF_INET, SOCK_STREAM, SOL_TCP);
        if ($sock == false) {
            echo "socket_create()失败:" . socket_strerror(socket_last_error($sock)) . "\n";
        } else {
            echo "socket_create()成功\n";
        }

        $port = $port->intValue() . "";#不然不行，坑啊

        $result = socket_connect($sock, '127.0.0.1', intval($port));
        if ($result == false) {
            echo "socket_connect()失败:" . socket_strerror(socket_last_error($sock)) . "\n";
        } else {
            echo "socket_connect()成功\n";
        }
        $stream = new sock_input_stream($sock);
      #  if ($deserializer == null) {
        $deserializer = new utf8_deserializer();
      #  }
        $item_array = $deserializer->load_stream($stream);
        #socket_close($sock);#改成yield之后不能关了
        return $item_array;
    }


    function getJavaStorageLevel($storageLevel){
        if(!($storageLevel instanceof StorageLevel)) {
            throw new Exception("storageLevel must be of type StorageLevel");
        }
        return $this->php_call_java->newStorageLevel($storageLevel->useDisk,
            $storageLevel->useMemory,
            $storageLevel->useOffHeap,
            $storageLevel->deserialized,
            $storageLevel->replication);
    }

    function stop(){
        $this->jsc->stop();
        $this->jsc=null;
        $this->accumulator_server->shutdown();
        $this->accumulator_server = null;
        exit();
    }

}

