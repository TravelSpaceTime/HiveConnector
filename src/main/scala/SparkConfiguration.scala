import org.apache.spark.sql.SparkSession

object SparkConfiguration {
  def sparkSqlConfig(): SparkSession = {
    val spark = SparkSession.builder.appName("AvailabilityPredictor")
      .master("local[4]")
      .config("spark.executor.memory", "8g")
      .config("spark.driver.memory", "8g")
      .config("spark.executor.memoryOverhead", "8g")
      .config("spark.driver.memoryOverhead", "8g")
      .config("spark.memory.offHeap.enabled", "true")
      .config("spark.memory.offHeap.size", "8g")
      .config("spark.kryoserializer.buffer.max", "1000m")
      .config("spark.kryoserializer.buffer", "200m")
      .config("spark.executor.extraJavaOptions", "-XX:MaxPermSize=4G -XX:+UseG1GC")
      .config("spark.driver.extraJavaOptions", "-XX:MaxPermSize=8G -Xms=8G -Xmx=8G")
      .config("spark.memory.fraction", "0.4")
      .config("spark.shuffle.memoryFraction","0.4")
      .config("spark.sql.adaptive.coalescePartitions.enabled", "false")
      .config("hadoop.security.authentication", "Kerberos")
    //  .config("spark.kerberos.keytab","/Users/shrikanth.mysore/Downloads/TPData/KerberosFiles/keytabs/diuapiuser.headless.keytab")
     // .config("spark.kerberos.principal","hive/shldvdpdh037.tvlport.net@TDPPRODSECURE.COM")
     // .config("spark.executor.extraJavaOptions","-Djava.security.krb5.conf=/Users/shrikanth.mysore/Downloads/TPData/KerberosFiles/krb5.conf")
      //.config("spark.driver.extraJavaOptions","-Djava.security.krb5.conf=/Users/shrikanth.mysore/Downloads/TPData/KerberosFiles/krb5.conf")
      .enableHiveSupport()
    .getOrCreate()
    System.setProperty("sun.security.krb5.debug", "true")

    val conf = spark.sparkContext
    //conf.getConf.registerKryoClasses(Array(classOf[OnDconnectionAnalysis]))
    conf.setLogLevel("ERROR")

    spark
  }


}
