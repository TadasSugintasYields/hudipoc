package com.github.leafty.hudi

import org.apache.spark.sql.functions.{col, concat_ws, lit}
import org.apache.spark.sql.{Column, DataFrame}


class AcquisitionsMultipleFieldPartitionDatasetDef(location: Option[String] = None)
  extends DatasetDef("acquisitions", HoodieKeys.ROW_KEY, "start_date", location) {

  final val ID = "id"

  object implicits extends AcquisitionsRowTransformations {

    implicit class Mapper(df : DataFrame) extends DatasetMapperFromRaw(df) {

      override def rowKeyColumn: Column = col(ID)

      override def partitionColumn: Column = concat_ws("/", col("seller"), col("prod_type"))
    }
  }
}

