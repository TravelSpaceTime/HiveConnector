import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.security.UserGroupInformation


object TheMain extends App{

  val t1 = System.nanoTime

  val spark = SparkConfiguration.sparkSqlConfig()



  val conf: Configuration = new Configuration
  conf.set("hadoop.security.authentication", "Kerberos")
  val krb5ConfigFilePath = "/Users/shrikanth.mysore/Downloads/TPData/KerberosFiles/krb5.conf";
  UserGroupInformation.setConfiguration(conf)
  System.setProperty("java.security.krb5.conf", krb5ConfigFilePath)
  System.setProperty("sun.security.krb5.debug", "true")

  UserGroupInformation.loginUserFromKeytab(
    "diuapispuser@TDPPRODSECURE.COM",
    "/Users/shrikanth.mysore/Downloads/TPData/KerberosFiles/keytabs/diuapispuser.headless.keytab")
  //UserGroupInformation.loginUserFromKeytab("telemetry@TVL.COM", "/Users/shrikanth.mysore/Downloads/TPData/kerbfilesFor001Server/telemetry.keytab")

  val jdbcUrl = "jdbc:hive2://shlpndpdh037.tvlport.net:2181" +
    ",shlpndpdh038.tvlport.net:2181" +
    ",shlpndpdh046.tvlport.net:2181/default;" +
    "principal=hive/shlpndpdh037.tvlport.net@TDPPRODSECURE.COM;" +
    "serviceDiscoveryMode=zooKeeper;" +
    "zooKeeperNamespace=hiveserver2"

  //val jdbcUrl = "jdbc:hive2://shldvdpdh001.tvlport.net:2181,shldvdpdh006.tvlport.net:2181,shldvdpdh007.tvlport.net:2181/default;password=root;principal=hive/shldvdpdh001.tvlport.net@TVL.COM;serviceDiscoveryMode=zooKeeper;user=root;zooKeeperNamespace=hiveserver2"
  val tableName = "uapi.agency_name_dim" //pf002_telemetry.car_availability_associated_charges
  val db2driver = "org.apache.hive.jdbc.HiveDriver"
  val query="show databases"


//val sqlQuery = s"select * from pf002_telemetry.car_availability_associated_charges limit 10"

  val jdbcDF = spark.read
    .format("jdbc")
    .option("url", jdbcUrl)
   .option("driver", "org.apache.hive.jdbc.HiveDriver")
    .option("query", query)
    .load()

  jdbcDF.foreach(a => println(a) )

  spark.stop()

  val duration = (System.nanoTime - t1) / 1e9d
  println("\n\ntotal Execution Time : " + duration + " in seconds\n\n")

}

/*

    /opt/homebrew/bin/spark-submit --principal hive/shldvdpdh001.tvlport.net@TVL.COM --keytab /Users/shrikanth.mysore/Downloads/TPData/kerbfilesFor001Server/telemetry.keytab  /Users/shrikanth.mysore/Documents/GitProjects/HiveConnector/target/scala-2.12/HiveConnector_2.12-0.1.0-SNAPSHOT.jar

*/