package com.gitlab.leafty.test.hudi

import com.github.leafty.hudi._
import com.uber.hoodie.DataSourceWriteOptions
import com.uber.hoodie.hive.SlashToDashPartitionValueExtractor

/**
  *
  */
class PartitionWriteSpec extends AsyncBaseSpec {

  import org.apache.log4j.{Level, Logger}

  lazy val log = Logger.getLogger("hudi.test")

  Logger.getLogger("org.apache").setLevel(Level.INFO)
  Logger.getLogger("com.uber.hoodie").setLevel(Level.INFO)

  import org.apache.spark.sql.{DataFrame, SparkSession}

  lazy implicit val spark: SparkSession = SparkBuilder.getSparkSession
  lazy val acquisitions = getAcquisitions
  lazy val acquisitionsMultipleFieldPartition = getAcquisitionsMultiplePartitionDs

  val acquisitionsDs = new AcquisitionsDatasetDef(Some(SparkBuilder.tmpLocation))
  val acquisitionsMultipleFieldPartitionDs = new AcquisitionsMultipleFieldPartitionDatasetDef(Some(SparkBuilder.tmpLocation))


  import DataSetDef._

  "hudi" should {

    "clean hive context" in {
      SparkBuilder.cleardatabase(spark)
      spark.sqlContext.tables(commonOpts(DataSourceWriteOptions.HIVE_DATABASE_OPT_KEY)).count() shouldBe 0

    }

    "hudi should name the partition field " in {
      // override new implicits with partition column name -
      // this partition column name should be in the dataset definition( DatasetDef)

      val newCommonOpts = Map(
        "hoodie.insert.shuffle.parallelism" -> "4",
        "hoodie.upsert.shuffle.parallelism" -> "4",
        DataSourceWriteOptions.PARTITIONPATH_FIELD_OPT_KEY -> HoodieKeys.PARTITION_KEY,
        DataSourceWriteOptions.HIVE_DATABASE_OPT_KEY-> "testdb",
        DataSourceWriteOptions.HIVE_URL_OPT_KEY -> "jdbc:hive2://172.18.0.1:10000",
        DataSourceWriteOptions.HIVE_USER_OPT_KEY-> "hive",
        DataSourceWriteOptions.HIVE_PASS_OPT_KEY-> "hive",
        DataSourceWriteOptions.HIVE_SYNC_ENABLED_OPT_KEY-> "true",
        DataSourceWriteOptions.HIVE_PARTITION_EXTRACTOR_CLASS_OPT_KEY->classOf[SlashToDashPartitionValueExtractor].getCanonicalName,

        //partition column
        DataSourceWriteOptions.HIVE_PARTITION_FIELDS_OPT_KEY -> "seller"

      )

      val df = acquisitions

      acquisitionsDs.writeReplace(df)(newCommonOpts)

      acquisitionsDs.listCommitsSince("000").length shouldBe 1

      val read = acquisitionsDs.read()

      val partitions =spark.sql("show partitions testdb.acquisitions")
      //partitions start should look like
      /*+--------------------+
        |           partition|
        +--------------------+
        |seller=seller-AMT...|
        |seller=seller-CIT...|
        |seller=seller-FIR...|
        |seller=seller-JPM...|
        | seller=seller-OTHER|
        +--------------------+*/
      partitions.filter(partitions("partition").startsWith("seller")).count() shouldBe 5

    }
  }

  "hudi should use multiple partitions " ignore {
    //test does not work due to the way hudi builds sql.

    /*  ALTER TABLE testdb.acquisitions ADD IF NOT EXISTS
     PARTITION (seller='AMTRUSTBANK'prod_type='FRM') <-- it needs a "," between the 2 definitions -> seller='AMTRUSTBANK',prod_type='FRM'
     LOCATION 'hdfs://172.18.0.1:8020/test/1536696545761_731/AMTRUSTBANK/FRM'
     PARTITION (seller='CITIMORTGAGE_INC.'prod_type='FRM')
     LOCATION 'hdfs://172.18.0.1:8020/test/1536696545761_731/CITIMORTGAGE_INC./FRM'
     PARTITION (seller='FIRSTTENNESSEEBANKNATIONALASSOCIATION'prod_type='FRM')
     LOCATION 'hdfs://172.18.0.1:8020/test/1536696545761_731/FIRSTTENNESSEEBANKNATIONALASSOCIATION/FRM'
     PARTITION (seller='JPMORGANCHASEBANK_NA'prod_type='FRM')
     LOCATION 'hdfs://172.18.0.1:8020/test/1536696545761_731/JPMORGANCHASEBANK_NA/FRM'
     PARTITION (seller='OTHER'prod_type='FRM')
     LOCATION 'hdfs://172.18.0.1:8020/test/1536696545761_731/OTHER/FRM' */


    SparkBuilder.cleardatabase(spark)

    // override new implicits with partition column name -
    // this partition column name should be in the dataset definition( DatasetDef)

    val newCommonOpts = Map(
      "hoodie.insert.shuffle.parallelism" -> "4",
      "hoodie.upsert.shuffle.parallelism" -> "4",
      DataSourceWriteOptions.PARTITIONPATH_FIELD_OPT_KEY -> HoodieKeys.PARTITION_KEY,
      DataSourceWriteOptions.HIVE_DATABASE_OPT_KEY-> "testdb",
      DataSourceWriteOptions.HIVE_URL_OPT_KEY -> "jdbc:hive2://172.18.0.1:10000",
      DataSourceWriteOptions.HIVE_USER_OPT_KEY-> "hive",
      DataSourceWriteOptions.HIVE_PASS_OPT_KEY-> "hive",
      DataSourceWriteOptions.HIVE_SYNC_ENABLED_OPT_KEY-> "true",

      //need to change the partition value extractor - this takes the partitionPath and splits it into the actual number of fields
      // that are part of the partition -i.e. 1 field in partition - the result of this class should have size 1, if 2 => 2
      DataSourceWriteOptions.HIVE_PARTITION_EXTRACTOR_CLASS_OPT_KEY->classOf[SlashSplitsFieldPartitionValueExtractor].getCanonicalName,

      //we change the partition so that it's defined as seller and start_date
      DataSourceWriteOptions.HIVE_PARTITION_FIELDS_OPT_KEY -> "seller,prod_type"

    )

    val df = acquisitionsMultipleFieldPartition

    acquisitionsMultipleFieldPartitionDs.writeReplace(df)(newCommonOpts)

    acquisitionsMultipleFieldPartitionDs.listCommitsSince("000").length shouldBe 1

    val read = acquisitionsDs.read()

    val partitions =spark.sql("show partitions testdb.acquisitions")
    //TODO change test after hudi fix.
    partitions.filter(partitions("partition").startsWith("seller")).count() shouldBe 5

  }

  def getAcquisitions: DataFrame = {

    val url  = "ds_0001"

    import acquisitionsDs.implicits._

    spark.read
      .format("csv")
      .option("header", "true")
      .load(SparkBuilder.hadoopNamenode + url)
      .mapFromRaw
  }

  def getAcquisitionsMultiplePartitionDs: DataFrame = {

    val url  = "ds_0001"

    import acquisitionsMultipleFieldPartitionDs.implicits._

    spark.read
      .format("csv")
      .option("header", "true")
      .load(SparkBuilder.hadoopNamenode + url)
      .mapFromRaw
  }


}
