/**
  * Owner: LBP.
  * Developer: Aimen SIJOUMI & Alison RAKOTOMAHEFA & Tyron FERREIRA
  * Date: 25/01/2019
  * Update: 26/02/2019
  */

package com.lbp.ingestion.task

import com.lbp.ingestion.config.Parameters
import com.lbp.ingestion.structure._
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * Write into HDFS, in the datalakeDiretory/folder.
  *
  * @param spark the sparkSession of this job.
  * @param storage the folder of the data.
  */
class WriteToHDFS(spark: SparkSession, storage: Storage.Value) extends Task {

  /**
    * Write a RDD of Row into HDFS, in the datalakeDiretory/folder.
    *
    * @param rdd represents the rdd to store.
    * @param path represents the output path file.
    */
  def writeToHDFS(rdd: RDDType, path: String, delimiter: Option[String]): Unit = {
    Logger.log.info(s"[${this.getClass.getSimpleName}] Write into $path")
    rdd match {
      case RDDRow(rddRow) => rddRow.map(row => row.mkString(delimiter.getOrElse(";"))).saveAsTextFile(path)
      case RDDString(rddString) => rddString.saveAsTextFile(path)
      case RDDDataFrame(rddDf) => writeToHDFS(rddDf, path, delimiter)
    }
  }

  /**
    * Write a Dataframe into HDFS, in the datalakeDiretory/folder.
    *
    * @param df represents the dataframe to store.
    * @param path represents the output path file.
    * @param delimiter reprensentes the separator of fields.
    */
  def writeToHDFS(df: DataFrame, path: String, delimiter: Option[String]): Unit = {
    Logger.log.info(s"[${this.getClass.getSimpleName}] Write into $path")
    df.write
      .format("csv")
      .option("header", "true").option("delimiter", delimiter.getOrElse(";"))
      .option("ignoreLeadingWhiteSpace","false")
      .option("ignoreTrailingWhiteSpace","false")
      .save(path.toLowerCase)
  }

  /**
    * @param flowFile represents the data of the current workflow.
    */
  override def process(flowFile: FlowFile): Unit = {
    val schema = flowFile.schema.get
    val datalakeDirectory = s"${Parameters.DATA_LAKE}/${schema.branch}"
    val path = s"${schema.sourceGroup}/${schema.tableName.toLowerCase}${if(flowFile.timeStamp.isDefined) s"/${flowFile.timeStamp.get}" else ""}/${flowFile.filename.toLowerCase}"

    storage match {
      case Storage.VALID =>
        this.writeToHDFS(flowFile.rdds(Storage.VALID), s"$datalakeDirectory/${Storage.VALID}/$path", schema.delimiter)
      case Storage.INVALID =>
        this.writeToHDFS(flowFile.rdds(Storage.INVALID), s"$datalakeDirectory/${Storage.INVALID}/$path", schema.delimiter)
      case Storage.ANONYMISED =>
        this.writeToHDFS(flowFile.rdds(Storage.ANONYMISED), s"$datalakeDirectory/${Storage.ANONYMISED}/$path", schema.delimiter)
    }
  }
}
