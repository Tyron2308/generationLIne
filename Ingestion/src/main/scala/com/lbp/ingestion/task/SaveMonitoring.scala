/**
  * Owner: LBP.
  * Developer: Aimen  & Alison RAKOTOMAHEFA
  * Date: 25/01/2019
  * Update: 22/02/2019
  */

package com.lbp.ingestion.task

import com.lbp.ingestion.structure._
import org.apache.spark.SparkContext
import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}

/**
  * Save the monitoring into Hive.
  *
  * @param sc the spark context of the job.
  * @param spark the sparkSession of this job.
  */
class SaveMonitoring(sc: SparkContext, spark: SparkSession) extends Task {

  val MonitoringDB = "monitoring"

  val SchemaMonitoring = StructType(List(
    StructField("filename", StringType, nullable = false),
    StructField("total_lines", LongType, nullable = false),
    StructField("valid_number", LongType, nullable = false),
    StructField("invalid_number", LongType, nullable = false)
  ))

  /**
    * @param flowFile represents the data of the current workflow.
    */
  override def process(flowFile: FlowFile): Unit = {

    val totalLines = flowFile.rdds(Storage.RAW) match {
      case RDDString(rdd) => rdd.count()
    }

    val validNumber = flowFile.rdds(Storage.VALID) match {
      case RDDDataFrame(df) => df.count()
    }

    Logger.log.info(s"[${this.getClass.getSimpleName}] Total valid lines: $validNumber")

    val invalidNumber = flowFile.rdds(Storage.INVALID) match {
      case RDDRow(rdd) => rdd.count()
    }

    Logger.log.info(s"[${this.getClass.getSimpleName}] Total invalid lines: $invalidNumber")

    val monitoringSeq = Seq(
      flowFile.filename,
      totalLines,
      validNumber,
      invalidNumber
    )

    Logger.log.info(s"[${this.getClass.getSimpleName}] Save monitoring for file ${flowFile.filename}")
    val monitoringDF = spark.createDataFrame(sc.parallelize(List(Row.fromSeq(monitoringSeq))), SchemaMonitoring)
    monitoringDF.write.mode("append").insertInto(s"$MonitoringDB.${flowFile.schema.get.sourceGroup.toLowerCase()}")
  }
}
