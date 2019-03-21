/**
  * Owner: LBP.
  * Developer: Aimen SIJOUMI & Alison RAKOTOMAHEFA
  * Date: 25/01/2019
  * Update: 26/02/2019
  */

package com.lbp.ingestion.task

import com.lbp.ingestion.structure._
import org.apache.spark.sql.SparkSession

/**
  * Create a dataframe if lines equals to valid
  * Create a RDD if lines equals to inalid
  *
  * @param spark the sparkSession of this job.
  * @param lines represents "valid" or "invalid" lines.
  */
class ProcessLines(spark: SparkSession, lines: Storage.Value) extends Task {

  /**
    * @param flowFile represents the data of the current workflow.
    */
  override def process(flowFile: FlowFile): Unit = {

    lines match {
      case Storage.VALID =>
        Logger.log.info(s"[${this.getClass.getSimpleName}] Create DataFrame for valid lines.")

        flowFile.rdds += (Storage.VALID -> RDDDataFrame(spark.createDataFrame(
          flowFile.validateRecord.get.filter(t => t._1 == Storage.VALID).map(t => t._2),
          flowFile.dfSchema.get
        )))
      case Storage.INVALID =>
        Logger.log.info(s"[${this.getClass.getSimpleName}] Create RDD for invalid lines.")
        flowFile.rdds += (Storage.INVALID -> RDDRow(flowFile.validateRecord.get.filter(t => t._1 == Storage.INVALID).map(t => t._2)))
    }
  }
}
