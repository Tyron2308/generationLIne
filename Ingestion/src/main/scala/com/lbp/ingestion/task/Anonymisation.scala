/**
  * Owner: LBP.
  * Developer: Aimen SIJOUMI & Alison RAKOTOMAHEFA
  * Date: 25/01/2019
  * Update: 26/02/2019
  */

package com.lbp.ingestion.task

import com.lbp.ingestion.structure.{FlowFile, Logger, RDDDataFrame, Storage}
import org.apache.spark.sql.SparkSession

/**
  * Anonymization of lines for hive table. This will select all lines and anonymize some values.
  *
  * @param spark the sparkSession of this job.
  */
class Anonymisation(spark: SparkSession) extends Task {

  /**
    * @param flowFile represents the data of the current workflow.
    */
  override def process(flowFile: FlowFile): Unit = {

    Logger.log.info(s"[${this.getClass.getSimpleName}] Anonymization of lines for table ${flowFile.schema.get.tableName.toLowerCase}")

    flowFile.rdds += (Storage.ANONYMISED -> RDDDataFrame(spark.createDataFrame(
      rowRDD = spark.sql(flowFile.schema.get.query.get).rdd,
      schema = flowFile.dfSchema.get
    )))
  }
}
