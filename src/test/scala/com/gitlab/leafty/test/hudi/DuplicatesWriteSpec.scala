package com.gitlab.leafty.test.hudi

import java.time.Instant

import com.github.leafty.hudi._
import com.uber.hoodie.DataSourceWriteOptions
import org.apache.hadoop.hive.conf.HiveConf

import scala.util.Random

/**
  *
  */
class DuplicatesWriteSpec extends AsyncBaseSpec {

  import org.apache.log4j.{Level, Logger}

  lazy val log = Logger.getLogger("hudi.test")

  Logger.getLogger("org.apache").setLevel(Level.INFO)
  Logger.getLogger("com.uber.hoodie").setLevel(Level.INFO)

  import org.apache.spark.sql.{DataFrame, Row, SparkSession}

  lazy implicit val spark: SparkSession = SparkBuilder.getSparkSession
  lazy val acquisitionsDup = getAcquisitions_Dup
  lazy val acquisitionsDupFull = getAcquisitions_DupFull
  lazy val acquisitionsDup1Row = getAcquisitions_Dup1Row
  lazy val acquisitionsDup2Row = getAcquisitions_Dup2Row

  val acquisitionsDupDs = new AcquisitionsDatasetDef(Some(SparkBuilder.tmpLocation))
  val acquisitionsDupFullDs = new AcquisitionsDatasetDef(Some(SparkBuilder.tmpLocation))
  val acquisitionsDupRowDs = new AcquisitionsDatasetDef(Some(SparkBuilder.tmpLocation))


  import DataSetDef._

  "hudi" should {

    "clean hive context" in {
      SparkBuilder.cleardatabase(spark)
      spark.sqlContext.tables(commonOpts(DataSourceWriteOptions.HIVE_DATABASE_OPT_KEY)).count() shouldBe 0

    }

    "ingest  partial duplicate 'acquisitions' " in {

      //rowkey 100011322040 is duplicate but has precombine key (start_date column) different

      val df = acquisitionsDup

      acquisitionsDupDs.writeReplace(df)

      acquisitionsDupDs.hasNewCommits("000") shouldBe true

      acquisitionsDupDs.listCommitsSince("000").length shouldBe 1

      acquisitionsDupDs.read().count() shouldBe df.count() - 1
      val read = acquisitionsDupDs.read()

        read.filter(read("id")==="100011322040" && read("start_date")==="12/1999").count() shouldBe 1

    }

    "ingest  full duplicate 'acquisitions' " in {

      //rowkey 100011322040 is duplicate also precombine key is the same

      val df = acquisitionsDupFull

      acquisitionsDupFullDs.writeReplace(df)

      acquisitionsDupFullDs.hasNewCommits("000") shouldBe true

      acquisitionsDupFullDs.listCommitsSince("000").length shouldBe 1

      acquisitionsDupFullDs.read().count() shouldBe df.count() - 1
      val read = acquisitionsDupFullDs.read()
    //first row encountered is the one added to the database
      read.filter(read("id")==="100011322040" && read("start_date")==="11/1999").count() shouldBe 1
      read.filter(read("id")==="100011322040" && read("start_date")==="11/1999" && read("int_rate")==="7.75").count() shouldBe 1

    }

    "ingest 2 duplicate rows 'acquisitions' with append " in {

      SparkBuilder.cleardatabase(spark)
      //writing first row.
      val df1 = acquisitionsDup1Row

      acquisitionsDupRowDs.writeReplace(df1)

      acquisitionsDupRowDs.read().count() shouldBe df1.count()

      val df2 = acquisitionsDup2Row
      //writing 2nd row which has the same row_key and precombine key but different int_rate
      acquisitionsDupRowDs.writeAppend(df2)

      val read = acquisitionsDupRowDs.read()

      //we have a duplicate insertion - same row key and same precondition ( start_date)
      read.filter(read("id")==="100011322040" && read("start_date") === "12/1999").count() shouldBe 2

    }

    "ingest 2 duplicate rows 'acquisitions' with upsert " in {

      SparkBuilder.cleardatabase(spark)
      //writing first row.
      val df1 = acquisitionsDup1Row

      acquisitionsDupRowDs.writeReplace(df1)

      acquisitionsDupRowDs.read().count() shouldBe df1.count()

      val df2 = acquisitionsDup2Row
      //writing 2nd row which has the same row_key and precombine key but different int_rate
      acquisitionsDupRowDs.writeUpsert(df2)

      val read = acquisitionsDupRowDs.read()

      //we have a duplicate insertion - same row key and same precondition ( start_date)
      val filtered = read.filter(read("id")==="100011322040" && read("start_date") === "12/1999")

      filtered.count() shouldBe 1
      //int_rate column is discriminator - has different value .
      filtered.select("int_rate").head().get(0) shouldBe "10.25"

    }
  }

  def getAcquisitions_Dup: DataFrame = {

    val url  = "ds_0001_duplicate_key"
    getAcquisitionsDs(url)
  }

  def getAcquisitions_DupFull: DataFrame = {

    val url  = "ds_0001_duplicate_all"
    getAcquisitionsDs(url)
  }
  def getAcquisitions_Dup1Row: DataFrame = {

    val url  = "ds_0001_duplicate_row_1"
    getAcquisitionsDs(url)
  }
  def getAcquisitions_Dup2Row: DataFrame = {

    val url  = "ds_0001_duplicate_row_2"
    getAcquisitionsDs(url)
  }


  private def getAcquisitionsDs(url: String) = {
    import acquisitionsDupDs.implicits._
    spark.read
      .format("csv")
      .option("header", "true")
      .load(SparkBuilder.hadoopNamenode + url)
      .mapFromRaw
  }
}
