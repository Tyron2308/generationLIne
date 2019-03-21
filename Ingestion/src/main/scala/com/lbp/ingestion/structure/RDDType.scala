package com.lbp.ingestion.structure

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row}

abstract class RDDType

case class RDDString(rdd: RDD[String]) extends RDDType
case class RDDRow(rdd: RDD[Row]) extends RDDType
case class RDDDataFrame(df: DataFrame) extends RDDType

