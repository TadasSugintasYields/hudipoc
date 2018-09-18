package com.github.leafty.hudi

import com.uber.hoodie.common.util.FSUtils
import com.uber.hoodie.config.HoodieWriteConfig
import com.uber.hoodie.{DataSourceReadOptions, DataSourceWriteOptions, HoodieDataSourceHelpers}
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}


object HoodieKeys {

  val ROW_KEY: String = "_row_key"
  val PARTITION_KEY: String = "partition"

}

/**
  * Defines a Hoodie dataaset
  *
  * @param name table name
  * @see [[HoodieWriteConfig.TABLE_NAME]]
  * @param rowKey name of field uniquely identifying a record.
  *               If there is no natural/business "id" then this can be synthesized from multiple fields.
  * @see [[DataSourceWriteOptions.RECORDKEY_FIELD_OPT_KEY]]
  * @param mergeByKey name of field used to merge updates (records with same [[rowKey]]) in different ... updates
  * @see [[DataSourceWriteOptions.PRECOMBINE_FIELD_OPT_KEY]]
  */
abstract case class DatasetDef(name: String, rowKey: String, mergeByKey: String, location: Option[String] = None) {

  /**
    * #todo How is [[DataSourceWriteOptions.PRECOMBINE_FIELD_OPT_KEY]] used exactly?
    * Is it for merging updates?
    */
  protected def asMap: Map[String, String] = Map(
    HoodieWriteConfig.TABLE_NAME -> name,
    DataSourceWriteOptions.RECORDKEY_FIELD_OPT_KEY -> rowKey,
    DataSourceWriteOptions.PRECOMBINE_FIELD_OPT_KEY -> mergeByKey,
    DataSourceWriteOptions.HIVE_TABLE_OPT_KEY -> name

  )

  /**
    *
    */
  def hasNewCommits(commitTimestamp: String)(implicit session: SparkSession): Boolean = {
    import DataSetDef._
    HoodieDataSourceHelpers.hasNewCommits(getFs(location.get), location.get, commitTimestamp)
  }

  /**
    *
    */
  def latestCommit(implicit session: SparkSession): String = {
    import DataSetDef._
    HoodieDataSourceHelpers.latestCommit(getFs(location.get), location.get)
  }

  /**
    *
    */
  def listCommitsSince(instantTimestamp: String)(implicit session: SparkSession): List[String] = {
    import DataSetDef._

    import scala.collection.JavaConverters._
    HoodieDataSourceHelpers.listCommitsSince(getFs(location.get), location.get, instantTimestamp).asScala.toList
  }

  private val uberHoodieFormat = "com.uber.hoodie"

  /**
    *
    */
  def writeReplace(df: DataFrame)(implicit commonOpts: Map[String, String]): Unit =
    df.write
      .format(uberHoodieFormat)
      .options(commonOpts)
      .options(this.asMap)
      .option(DataSourceWriteOptions.OPERATION_OPT_KEY, DataSourceWriteOptions.INSERT_OPERATION_OPT_VAL)
      .mode(SaveMode.Overwrite)
      .save(this.location.get)

  /**
    *
    */
  def writeAppend(df: DataFrame)(implicit commonOpts: Map[String, String]): Unit =
    df.write
      .format(uberHoodieFormat)
      .options(commonOpts)
      .options(this.asMap)
      .option(DataSourceWriteOptions.OPERATION_OPT_KEY, DataSourceWriteOptions.INSERT_OPERATION_OPT_VAL)
      .mode(SaveMode.Append)
      .save(this.location.get)

  /**
    * #todo Is this api really needed or there is a better way?
    * Why UPSERT only here or for all "appends"?
    */
  def writeUpsert(df: DataFrame)(implicit commonOpts: Map[String, String]): Unit =
    df.write
      .format(uberHoodieFormat)
      .options(commonOpts)
      .options(this.asMap)
      .option(DataSourceWriteOptions.OPERATION_OPT_KEY, DataSourceWriteOptions.UPSERT_OPERATION_OPT_VAL)
      .mode(SaveMode.Append)
      .save(this.location.get)

  /**
    *
    * @param commitTime a real commit or will real all
    */
  def read(commitTime: Option[String] = None)(implicit session: SparkSession): DataFrame = {
    commitTime match {
      case Some(c) ⇒ session.read
        .format(uberHoodieFormat)
        .option(DataSourceReadOptions.VIEW_TYPE_OPT_KEY, DataSourceReadOptions.VIEW_TYPE_INCREMENTAL_OPT_VAL)
        .option(DataSourceReadOptions.BEGIN_INSTANTTIME_OPT_KEY, c)
        .load(this.location.get)

      case None ⇒ session.read
        .format(uberHoodieFormat)
        .load(this.location.get + "/*/*/*")
    }
  }
}

object DataSetDef {

  lazy implicit val commonOpts = Map(
    "hoodie.insert.shuffle.parallelism" -> "4",
    "hoodie.upsert.shuffle.parallelism" -> "4",
    DataSourceWriteOptions.PARTITIONPATH_FIELD_OPT_KEY -> HoodieKeys.PARTITION_KEY,
    //TODO - MOVE HIVE INFORMATION FROM HERE
    //what is it with the IP - we need to change it .
    DataSourceWriteOptions.HIVE_DATABASE_OPT_KEY-> "testdb",
    DataSourceWriteOptions.HIVE_URL_OPT_KEY -> "jdbc:hive2://172.18.0.1:10000",
    DataSourceWriteOptions.HIVE_PARTITION_FIELDS_OPT_KEY -> "dateStr",
    DataSourceWriteOptions.HIVE_USER_OPT_KEY-> "hive",
    DataSourceWriteOptions.HIVE_PASS_OPT_KEY-> "hive",
    DataSourceWriteOptions.HIVE_SYNC_ENABLED_OPT_KEY-> "true",
    DataSourceWriteOptions.HIVE_PARTITION_EXTRACTOR_CLASS_OPT_KEY->classOf[SlashToDashPartitionValueExtractor].getCanonicalName

  )

  def getFs(path: String)(implicit session: SparkSession) = FSUtils.getFs(path, session.sparkContext.hadoopConfiguration)
}

