package com.gitlab.leafty.test.hudi

import com.github.leafty.hudi.DataSetDef.commonOpts
import com.github.leafty.hudi._
import com.uber.hoodie.DataSourceWriteOptions

/**
  *
  */
class FannieMaeHudiSpec extends AsyncBaseSpec {

  import org.apache.log4j.{Level, Logger}

  lazy val log = Logger.getLogger("hudi.test")

  Logger.getLogger("org.apache").setLevel(Level.WARN)
  Logger.getLogger("com.uber.hoodie").setLevel(Level.WARN)

  import org.apache.spark.sql.{DataFrame, Row, SparkSession}

  lazy implicit val spark: SparkSession = SparkBuilder.getSparkSession
  lazy val acquisitionsAll = getAcquisitions_All
  lazy val performancesAll = getPerformances_All
  lazy val performancesAll_counts = List(40, 16, 40, 58, 31, 12, 51, 36)
  lazy val acquisitions_2Split = getAcquisitions_2Split
  lazy val performances_3Split = getPerformances_3Split

  val db = commonOpts(DataSourceWriteOptions.HIVE_DATABASE_OPT_KEY)


  "test data sets" should {
    "contain acquisitions" in {

      val df = acquisitionsAll
      df.count() shouldBe 8
    }

    "contain performances" in {
      val df = performancesAll

      // row counts from each csv file
      val count = performancesAll_counts.sum

      df.count() shouldBe count
    }

    "contain acquisitions splits" in {
      val dfs = acquisitions_2Split

      dfs(0).columns should contain theSameElementsAs acquisitionsAll.columns
      dfs(1).columns should contain theSameElementsAs acquisitionsAll.columns

      dfs(0).count() shouldBe 4
      dfs(1).count() shouldBe 4
    }

    "contain performances splits" in {
      val map = performances_3Split
      map should have size 8

      // row counts from each csv file
      val counts = performancesAll_counts
      val sizes = for {(df1, df2, df3) <- map.values} yield {
        df1.count() + df2.count() + df3.count()
      }

      sizes should contain theSameElementsAs counts
    }
  }

  val acquisitionsDs = new AcquisitionsDatasetDef(Some(SparkBuilder.tmpLocation))

  val performancesDs = new PerformancesDatasetDef(Some(SparkBuilder.tmpLocation))


  def getIds(df: DataFrame, id: String, distinct: Boolean = true): Array[String] = {
    (if (distinct)
      df.select(df(id)).distinct()
    else
      df.select(df(id))).collect().map(_.getString(0))
  }


  import DataSetDef._

  "hudi" should {

    "clean hive context" in {
      SparkBuilder.cleardatabase(spark)
      spark.sqlContext.tables(commonOpts(DataSourceWriteOptions.HIVE_DATABASE_OPT_KEY)).count() shouldBe 0
    }

    import scala.concurrent.Promise
    object CommitRuntime {
      var performancesCommits: List[Promise[String]] = List.empty

      def init = performancesCommits = List(Promise(), Promise(), Promise())
    }

    /**
      * Ingest the first part
      */
    "ingest 'acquisitions': first 1/2" in {

      CommitRuntime.init

      val df = acquisitions_2Split.head

      acquisitionsDs.writeReplace(df)

      acquisitionsDs.hasNewCommits("000") shouldBe true

      acquisitionsDs.listCommitsSince("000").length shouldBe 1

      acquisitionsDs.read().count() shouldBe df.count()
    }

    "hive context should have the acquisitions tables" in {
      spark.sqlContext.tables(commonOpts(DataSourceWriteOptions.HIVE_DATABASE_OPT_KEY)).head.get(1) shouldBe "acquisitions"
      spark.sql(s"select * from $db.acquisitions").show()
      spark.sql(s"select count(*) from $db.acquisitions").head().get(0) shouldBe 4

    }


    "for (1), ingest their 'performances': first 1/3 rd" in {

      // Group 1 from acquisitions, 1st third
      val acquisitionsDf = acquisitions_2Split.head

      val ids = getIds(acquisitionsDf, "id")

      val map = performances_3Split
      val dfs = for {id <- ids} yield map(id)._1

      val insertDf = joinAll(dfs)

      log.info(
        s"""For `acquisitions` ${ids.mkString(", ")}
           ingest `performances` ${getIds(insertDf, performancesDs.ID, false).mkString(", ")}""".stripMargin)

      performancesDs.writeReplace(insertDf)

      CommitRuntime.performancesCommits(0).success(performancesDs.latestCommit)

      performancesDs.listCommitsSince("000").length shouldBe 1

      performancesDs.read().count() shouldBe insertDf.count()
    }

    "hive context should have the performances table" in {

      val df =spark.sqlContext.tables(commonOpts(DataSourceWriteOptions.HIVE_DATABASE_OPT_KEY)).select("tableName")
      df.filter(df("tableName")==="performances").count() shouldBe 1
      showPerformances
      getCountFromPerformances shouldBe 50

    }

    "for (1), ingest their 'performances': second 1/3 rd" in {
      // Group 1 from acquisitions, 2nd third
      val acquisitionsDf = acquisitions_2Split(0)
      val ids = getIds(acquisitionsDf, acquisitionsDs.ID)

      val map = performances_3Split
      val dfs = for {id <- ids} yield map(id)._2

      val insertDf = joinAll(dfs)

      log.info(
        s"""For `acquisitions` ${ids.mkString(", ")}
           ingest `performances` ${getIds(insertDf, performancesDs.ID, false).mkString(", ")}""".stripMargin)

      performancesDs.writeAppend(insertDf)

      CommitRuntime.performancesCommits(1).success(performancesDs.latestCommit)

      performancesDs.listCommitsSince("000").length shouldBe 2

      // read back data from hudi (using incremental view)
      //performancesCommitInstantTime1.isCompleted shouldBe true
      for {
        commitTime <- CommitRuntime.performancesCommits(0).future

        hudiDf = performancesDs.read(Some(commitTime))

      } yield {

        hudiDf.count() shouldBe insertDf.count()
      }
    }

    "hive context should see everything 1st 1/3rd and 2nd 1/3rd and should also see partially " in {

      //performances - table name for which to choose incremental mode
      setHiveModeToIncremental("performances")
      //fetch only one commit in incremental mode
      spark.sql("set hoodie.performances.consume.max.commits=1")

      spark.conf.get("hoodie.performances.consume.mode") shouldBe "INCREMENTAL"

      //each commit until now was 50 each
      val commits = performancesDs.listCommitsSince("000").zip(List(50,50))
      val expectedCounts =
      for{
        (timestamp,count) <- commits
      }  yield {
        spark.sql(s"set hoodie.performances.consume.start.timestamp=${timestamp.toLong-1} ")
        getCountFromPerformances shouldBe count
      }
      // setting mode to LATEST should yield full insert of 100
      setHiveModeToLatest("performances")
      getCountFromPerformances shouldBe 100

    }

    "for (1), ingest their 'performances': third 1/3 rd" in {

      // Group 1 from acquisitions, 3rd third
      val acquisitionsDf = acquisitions_2Split(0)
      val ids = getIds(acquisitionsDf, acquisitionsDs.ID)

      val map = performances_3Split
      val dfs = for {id <- ids} yield map(id)._3

      val insertDf = joinAll(dfs)

      log.info(
        s"""For `acquisitions` ${ids.mkString(", ")}
           ingest `performances` ${getIds(insertDf, performancesDs.ID, false).mkString(", ")}""".stripMargin)

      performancesDs.writeAppend(insertDf)

      CommitRuntime.performancesCommits(2).success(performancesDs.latestCommit)

      performancesDs.listCommitsSince("000").length shouldBe 3

      // read back data from hudi (using incremental view)
      //performancesCommitInstantTime1.isCompleted shouldBe true
      for {
        commitTime <- CommitRuntime.performancesCommits(1).future

        hudiDf = performancesDs.read(Some(commitTime))

      } yield {

        hudiDf.count() shouldBe insertDf.count()
      }

    }

    "hive context should see everything 1st 1/3rd , 2nd 1/3rd and 3rd 1/3rd" in {

      //performances - table name for which to choose incremental mode
      setHiveModeToIncremental("performances")
      //fetch only one commit in incremental mode
      spark.sql("set hoodie.performances.consume.max.commits=1")

      //each commit until now was 50 each, except the last having 54

      val commits = performancesDs.listCommitsSince("000").zip(List(50,50,54))
      val expectedCounts =
        for{
          (timestamp,count) <- commits
        }  yield {
          spark.sql(s"set hoodie.performances.consume.start.timestamp=${timestamp.toLong-1} ")
          getCountFromPerformances shouldBe count
        }
      // setting mode to LATEST should yield full insert of 150
      setHiveModeToLatest("performances")
      getCountFromPerformances shouldBe 154

    }

    "check have ingested first half of ds_0001 x ds_0002 consistently" in {

      acquisitionsDs.listCommitsSince("000").length shouldBe 1

      val acquisitionsDf = acquisitionsDs.read()

      performancesDs.listCommitsSince("000").length shouldBe 3

      val performancesDf = performancesDs.read()

      val joinedDf = acquisitionsDf.join(performancesDf, acquisitionsDf(acquisitionsDs.ID) === performancesDf(performancesDs.ID), "inner")

      joinedDf.count() shouldBe performancesDf.count()

      val a_ids = getIds(acquisitionsDf, acquisitionsDs.ID)
      val j_ids = getIds(joinedDf, acquisitionsDs.ID)

      a_ids should contain theSameElementsAs j_ids
    }

    /**
      * Ingest the second part
      */
    "ingest 'acquisitions': second 1/2" in {

      CommitRuntime.init

      val df = acquisitions_2Split(1)

      acquisitionsDs.writeAppend(df)

      acquisitionsDs.hasNewCommits("000") shouldBe true

      acquisitionsDs.listCommitsSince("000").length shouldBe 2

      acquisitionsDs.read().count() shouldBe acquisitionsAll.count()
    }

    "hive context should have both commits of the acquisition table" in {

      //performances - table name for which to choose incremental mode
      setHiveModeToIncremental("acquisitions")
      //fetch only one commit in incremental mode
      spark.sql("set hoodie.acquisitions.consume.max.commits=1")

      //each commit until now was 4 each
      val commits = performancesDs.listCommitsSince("000").zip(List(4,4))
      val expectedCounts =
        for{
          (timestamp,count) <- commits
        }  yield {
          spark.sql(s"set hoodie.acquisitions.consume.start.timestamp=${timestamp.toLong-1} ")
          spark.sql(s"select count(*) from $db.acquisitions").head().get(0) shouldBe count
        }

      // setting mode to LATEST should yield full insert of 150
      setHiveModeToLatest("acquisitions")
      spark.sql(s"select count(*) from $db.acquisitions").head().get(0) shouldBe 8

    }

    "for (2), ingest their 'performances': first 1/3 rd" in {

      // Group 1 from acquisitions, 1st third
      val acquisitionsDf = acquisitions_2Split(1)

      val ids = getIds(acquisitionsDf, acquisitionsDs.ID)

      val map = performances_3Split
      val dfs = for {id <- ids} yield map(id)._1

      val insertDf = joinAll(dfs)

      log.info(
        s"""For `acquisitions` ${ids.mkString(", ")}
           ingest `performances` ${getIds(insertDf, performancesDs.ID, false).mkString(", ")}""".stripMargin)

      performancesDs.writeAppend(insertDf)

      CommitRuntime.performancesCommits(0).success(performancesDs.latestCommit)

      performancesDs.listCommitsSince("000").length shouldBe 1 + 3

      //#todo - continue from previous 3 commits
      // performancesDs.read().count() shouldBe insertDf.count()
    }

    "for (2), ingest their 'performances': second 1/3 rd" in {
      // Group 1 from acquisitions, 2nd third
      val acquisitionsDf = acquisitions_2Split(1)
      val ids = getIds(acquisitionsDf, acquisitionsDs.ID)

      val map = performances_3Split
      val dfs = for {id <- ids} yield map(id)._2

      val insertDf = joinAll(dfs)

      log.info(
        s"""For `acquisitions` ${ids.mkString(", ")}
           ingest `performances` ${getIds(insertDf, performancesDs.ID, false).mkString(", ")}""".stripMargin)

      performancesDs.writeAppend(insertDf)

      CommitRuntime.performancesCommits(1).success(performancesDs.latestCommit)

      performancesDs.listCommitsSince("000").length shouldBe 2 + 3

      // read back data from hudi (using incremental view)
      //performancesCommitInstantTime1.isCompleted shouldBe true
      for {
        commitTime <- CommitRuntime.performancesCommits(0).future

        hudiDf = performancesDs.read(Some(commitTime))

      } yield {

        hudiDf.count() shouldBe insertDf.count()
      }
    }

    "for (2), ingest their 'performances': third 1/3 rd" in {

      // Group 1 from acquisitions, 3rd third
      val acquisitionsDf = acquisitions_2Split(1)
      val ids = getIds(acquisitionsDf, acquisitionsDs.ID)

      val map = performances_3Split
      val dfs = for {id <- ids} yield map(id)._3

      val insertDf = joinAll(dfs)

      log.info(
        s"""For `acquisitions` ${ids.mkString(", ")}
           ingest `performances` ${getIds(insertDf, performancesDs.ID, false).mkString(", ")}""".stripMargin)

      performancesDs.writeAppend(insertDf)

      CommitRuntime.performancesCommits(2).success(performancesDs.latestCommit)

      performancesDs.listCommitsSince("000").length shouldBe 3 + 3

      // read back data from hudi (using incremental view)
      //performancesCommitInstantTime1.isCompleted shouldBe true
      for {
        commitTime <- CommitRuntime.performancesCommits(1).future

        hudiDf = performancesDs.read(Some(commitTime))

      } yield {

        hudiDf.count() shouldBe insertDf.count()
      }
    }

    "check have ingested second half of ds_0001 x ds_0002 consistently" in {

      acquisitionsDs.listCommitsSince("000").length shouldBe 2

      val acquisitionsDf = acquisitionsDs.read()

      performancesDs.listCommitsSince("000").length shouldBe 6

      val performancesDf = performancesDs.read()

      val joinedDf = acquisitionsDf.join(performancesDf, acquisitionsDf(acquisitionsDs.ID) === performancesDf(performancesDs.ID), "inner")

      joinedDf.count() shouldBe performancesDf.count()

      val a_ids = getIds(acquisitionsDf, acquisitionsDs.ID)
      val j_ids = getIds(joinedDf, acquisitionsDs.ID)

      a_ids should contain theSameElementsAs j_ids

      performancesDf.count() shouldBe performancesAll.count()
    }

    "hive context should have all the rows after all ingestions" in {

      setHiveModeToLatest("performances")
      getCountFromPerformances shouldBe performancesAll.count()
      //and all of them should have the foreclosure amount null
      spark.sql(s"select count(*) from testdb.performances where foreclosure_amount is null ").head().get(0) shouldBe performancesAll.count()

      //and the first 6 commits have all the data
      setHiveModeToIncremental("performances")
      spark.sql("set hoodie.performances.consume.max.commits=6")
      spark.sql(s"set hoodie.performances.consume.start.timestamp=000")
      getCountFromPerformances shouldBe 284

    }

    "force updates to the last 1/3 rd" in  {
      val chunks = getPerformances_3Split_raw

      import performancesDs.implicits._
      import org.apache.spark.sql.functions.{col, lit}

      val updatedDfs = chunks map { m ⇒
          m.values map { values3 ⇒
            val values = values3._3
            //log.info("Schema " + values.schema.toString())
            values.withColumn("curr_date", curr_date_inc)
            values.withColumn("foreclosure_amount", lit(1234.56))
          }
      }

      updatedDfs.flatten foreach { updatedDf ⇒ performancesDs.writeUpsert(updatedDf)}

      // performancesDs.listCommitsSince("000").last
      val df = performancesDs.read()

      df.filter(col("foreclosure_amount").isNotNull).count() shouldBe 98 // #todo df.count() / 3
    }

    "hive context should have updates " in {
      //after UPSERT we should have 98 with foreclosure_amount not null
      setHiveModeToLatest("performances")
      spark.sql(s"select count(*) from $db.performances where foreclosure_amount is NOT null").head().get(0) shouldBe 98
      //the count should still be performancesAll.count()

      getCountFromPerformances shouldBe performancesAll.count()

      //the first 6 commits are missing the updated entries i.e. - performancesAll.count() - 98

      setHiveModeToIncremental("performances")
      //fetch only one commit in incremental mode
      spark.sql("set hoodie.performances.consume.max.commits=6")
      spark.sql(s"set hoodie.performances.consume.start.timestamp=000")
      //if we take only the initial 6 commits we should have everything except the last 8 commits done in the upserts above
      getCountFromPerformances shouldBe (performancesAll.count() -98)

    }
  }

  private def showPerformances = {
    spark.sql(s"select * from $db.performances").show()
  }

  private def getCountFromPerformances = {
    spark.sql(s"select count(*) from $db.performances").head().get(0)
  }

  private def setHiveModeToIncremental(table:String) = {
    spark.sql(s"set hoodie.$table.consume.mode=INCREMENTAL")
  }

  private def setHiveModeToLatest(table:String) = {
    spark.sql(s"set hoodie.$table.consume.mode=LATEST")
  }

  /**
    * @return acquisitions split in two halves
    */
  def getAcquisitions_2Split: List[DataFrame] = {
    val df1 = acquisitionsAll
    val thr = df1.count() / 2

    val rdd2 = df1.rdd.zipWithUniqueId()
    val rdd3_1 = rdd2.filter { case (_, rank) => rank < thr }.map(_._1)
    val rdd3_2 = rdd2.filter { case (_, rank) => rank >= thr }.map(_._1)

    val df4_1 = spark.createDataFrame(rdd3_1, df1.schema)
    val df4_2 = spark.createDataFrame(rdd3_2, df1.schema)

    List(df4_1, df4_2)
  }

  /**
    * @return performances, with each group split in thirds
    */
  def getPerformances_3Split: Map[String, (DataFrame, DataFrame, DataFrame)] = {

    val emptyMap = Map.empty[String, (DataFrame, DataFrame, DataFrame)]
    getPerformances_3Split_raw.fold(emptyMap) { (map1, map2) ⇒ map1 ++ map2 }
  }

  private def getPerformances_3Split_raw : List[Map[String, (DataFrame, DataFrame, DataFrame)]] = {
    val dfs = getPerformances

    dfs map { df ⇒

      val ids = getIds(df, performancesDs.ID)

      val mapped = for {id <- ids} yield id -> df.filter(df(performancesDs.ID) === id)

      val splitMapped = for {(id, df) <- mapped} yield {
        val rdd = df.rdd.zipWithUniqueId()

        val thr1 = df.count() / 3
        val thr2 = 2 * df.count() / 3

        val rdd_1 = rdd.filter { case (_, rank) => rank < thr1 }.map(_._1)
        val rdd_2 = rdd.filter { case (_, rank) => rank >= thr1 && rank < thr2 }.map(_._1)
        val rdd_3 = rdd.filter { case (_, rank) => rank >= thr2 }.map(_._1)

        val df_1 = spark.createDataFrame(rdd_1, df.schema)
        val df_2 = spark.createDataFrame(rdd_2, df.schema)
        val df_3 = spark.createDataFrame(rdd_3, df.schema)

        id -> (df_1, df_2, df_3)
      }

      splitMapped.toMap
    }
  }

  def getAcquisitions_All: DataFrame = {
    //getClass.getResource("/ds_0001") <--switch to this if using local.
    val url  = "ds_0001"
    import acquisitionsDs.implicits._

    spark.read
      .format("csv")
      .option("header", "true")
      .load(SparkBuilder.hadoopNamenode+url)
      .mapFromRaw
  }

  def getPerformances: List[DataFrame] = {
    val url  = "ds_0002"
    /**
      * #resource
      * https://spark.apache.org/docs/2.3.0/api/sql/index.html
      * https://docs-snaplogic.atlassian.net/wiki/spaces/SD/pages/2458071/Date+Functions+and+Properties+Spark+SQL
      */

    import performancesDs.implicits._

    (1 to 8).toList map { i ⇒
        spark.read
        .format("csv")
        .option("header", "true")
        .load(s"${SparkBuilder.hadoopNamenode+url}/raw_00$i.csv")
        .mapFromRaw.applyTransformations
    }
  }

  def getPerformances_All: DataFrame =
    joinAll(getPerformances)

  private def joinAll(dfs: Seq[DataFrame]) = {
    val emptyDF = spark.createDataFrame(spark.sparkContext.emptyRDD[Row], dfs(0).schema)
    dfs.fold(emptyDF) { (df1, df2) ⇒ df1.union(df2) }
  }

}
