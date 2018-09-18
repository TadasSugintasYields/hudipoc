package com.gitlab.leafty.test.hudi

import java.time.Instant

import com.github.leafty.hudi.DataSetDef.commonOpts
import com.uber.hoodie.DataSourceWriteOptions
import org.apache.hadoop.hive.conf.HiveConf
import org.apache.spark.sql.SparkSession

import scala.util.Random

object SparkBuilder {

  val ip ="172.18.0.1"
  val hadoopNamenode= s"hdfs://$ip:8020/"
  val HIVE_SERVER_JDBC_URL = s"jdbc:hive2://$ip:10000"

  def getSparkSession:SparkSession= {
    SparkSession.builder()
      .config("spark.ui.enabled", "false")
      .config("spark.driver.host", ip) //for workers to know the master
      .config("hive.metastore.local", "false") //metastore is somewhere else
      //following 3 are required by Hoodie for hive metastore location
      .config(HiveConf.ConfVars.METASTOREURIS.varname, s"thrift://$ip:9083")
      .config(HiveConf.ConfVars.HIVE_SERVER2_THRIFT_BIND_HOST.varname, s"$ip")
      .config(HiveConf.ConfVars.HIVE_SERVER2_THRIFT_PORT.varname, "9083")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      //disables inside spark parquet reading -and will use hive serde
      .config("spark.sql.hive.convertMetastoreParquet","false")
      //this is required so that we can execute sql's via spark context
      .config("spark.jars","/home/workspace1/.m2/repository/com/uber/hoodie/hoodie-hadoop-mr/0.4.3-y-SNAPSHOT/hoodie-hadoop-mr-0.4.3-y-SNAPSHOT.jar")
      .enableHiveSupport()
      .appName("test-hudi")
      .master("local[2]")
      .getOrCreate()

  }

  def tmpLocation: String = {
    s"${SparkBuilder.hadoopNamenode}test/${Instant.now.toEpochMilli}_${new Random().nextInt(1000)}"
  }


  def cleardatabase(spark:SparkSession):Unit = {

    spark.sql(s"drop database if exists ${commonOpts(DataSourceWriteOptions.HIVE_DATABASE_OPT_KEY)} cascade ").show()
    spark.sql(s"create database ${commonOpts(DataSourceWriteOptions.HIVE_DATABASE_OPT_KEY)}").show()

  }
}
