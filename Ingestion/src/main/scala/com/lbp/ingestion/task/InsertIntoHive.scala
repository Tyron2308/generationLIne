/**
  * Owner: LBP.
  * Developer: Aimen SIJOUMI & Alison RAKOTOMAHEFA
  * Date: 25/01/2019
  * Update: 26/02/2019
  */

package com.lbp.ingestion.task

import com.lbp.ingestion.structure.{FlowFile, Logger, RDDDataFrame, Storage}
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

/**
  * Write a dataframe into Hive, in the database and table.
  *
  * @param spark the sparkSession of this job.
  * @param storage represents "valid" or "anonymised".
  * @param mode specifies insert mode for the specific workflow. Append and Overwrite will be used.
  */
class InsertIntoHive(spark: SparkSession, storage: Storage.Value, mode: SaveMode) extends Task {

  def insert(df: DataFrame, database: String, table: String): Unit = {
    Logger.log.info(s"[${this.getClass.getSimpleName}] Insert lines into $database.$table")
    df.write.mode(this.mode).insertInto(s"$database.$table")
  }

  /**
    * @param flowFile represents the data of the current workflow.
    */
  override def process(flowFile: FlowFile): Unit = {

    val schema = flowFile.schema.get
    val database = schema.sourceGroup
    val tableName = s"${schema.tableName}${if(flowFile.timeStamp.isDefined) s"_${flowFile.timeStamp.get}" else ""}"

    storage match {
      case Storage.VALID => flowFile.rdds(Storage.VALID) match {
        case RDDDataFrame(df) => this.insert(df, s"${database}_${Storage.VALID}".toLowerCase, tableName.toLowerCase)
      }
      case Storage.ANONYMISED => flowFile.rdds(Storage.VALID) match {
        case RDDDataFrame(df) => this.insert(df, s"${database}_${Storage.ANONYMISED}".toLowerCase, tableName.toLowerCase)
      }
    }
  }
}
