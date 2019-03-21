/**
  * Owner: LBP.
  * Developer: Aimen SIJOUMI & Alison RAKOTOMAHEFA
  * Date: 25/01/2019
  * Update: 22/02/2019
  */

package com.lbp.ingestion.task

import com.lbp.ingestion.structure.{FlowFile, Logger, Storage}
import org.apache.spark.sql.SparkSession

/**
  * Create Hive tables for and without anonymization if needed.
  *
  * @param spark the sparkSession of this job.
  */
class CreateHiveTable(spark: SparkSession) extends Task {

  /**
    * @param spark the sparkSession of this job.
    * @param database the database (must exists).
    * @param table the table to create.
    */
  def createTable(spark: SparkSession, database: String, table: String, ddl: String): Unit = {
    Logger.log.info(s"[${this.getClass.getSimpleName}] Create table: ${database.toLowerCase()}.${table.toLowerCase()}")
    spark.sql(s"CREATE TABLE IF NOT EXISTS ${database.toLowerCase()}.${table.toLowerCase()} $ddl")
  }

  /**
    * @param flowFile represents the data of the current workflow.
    */
  override def process(flowFile: FlowFile): Unit = {

    val schema = flowFile.schema.get
    val database = schema.sourceGroup
    val tableName = s"${schema.tableName}${if(flowFile.timeStamp.isDefined) s"_${flowFile.timeStamp.get}" else ""}"

    this.createTable(spark, s"${database}_${Storage.VALID}", tableName, schema.ddl.get)

    if (flowFile.anonymisationLevel == 1) {
      this.createTable(spark, s"${database}_${Storage.ANONYMISED}", tableName, schema.ddl.get)
    }
  }
}
