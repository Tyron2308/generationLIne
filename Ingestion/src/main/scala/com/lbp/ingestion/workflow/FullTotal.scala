/**
  * Owner: LBP.
  * Developer: Aimen SIJOUMI & Alison RAKOTOMAHEFA
  * Date: 25/01/2019
  * Update: 26/02/2019
  */

package com.lbp.ingestion.workflow

import com.lbp.ingestion.config.{Config, Parameters}
import com.lbp.ingestion.structure.{FlowFile, RDDString, Storage}
import com.lbp.ingestion.task._
import org.apache.hadoop.fs.{FileStatus, FileSystem}
import org.apache.spark.SparkContext
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.storage.StorageLevel

/**
  * The FullTotal class extends Workflow.
  * For this workflow, old data will be removed and overwrite.
  *
  * @param fs represents the file system (from hadoop).
  * @param sc the spark context of the job.
  * @param spark the sparkSession of this job.
  */
class FullTotal(fs: FileSystem, sc: SparkContext, spark: SparkSession) extends Workflow {

  override var flowFile: FlowFile = _
  override var tasks: List[Task] = List()

  /**
    * @param fs represents the file system (from hadoop).
    * @param fileStatus the file status of the file.
    * @param sc the spark context of the job.
    * @param spark the sparkSession of this job.
    * @param config the config of the app.
    */
  def this(fs: FileSystem, fileStatus: FileStatus, sc: SparkContext, spark: SparkSession, singleFilename: String, config: Config) {
    this(fs, sc, spark)
    this.flowFile = new FlowFile(
      singleFilename = singleFilename,
      filename = fileStatus.getPath.toString.split("/").last,
      rdds = Map(Storage.RAW -> RDDString(sc.textFile(fileStatus.getPath.toString).persist(StorageLevel.MEMORY_AND_DISK))),
      header = config.header,
      startWith = config.startLineWith,
      anonymisationLevel = config.anLevel
    )

    tasks ++= List(
      new FetchAndEvaluateSchema(spark = spark),
      new DeleteFromHDFS(fs = fs, from = Parameters.DATA_LAKE),
      new ValidateRecord(),
      new ProcessLines(spark = spark, lines = Storage.VALID),
      new ProcessLines(spark = spark, lines = Storage.INVALID),
      new WriteToHDFS(spark = spark, storage = Storage.VALID),
      new InsertIntoHive(spark = spark, storage = Storage.VALID, mode = SaveMode.Overwrite),
      new WriteToHDFS(spark = spark, storage = Storage.INVALID)
    )

    if (config.anLevel == 1) {
      tasks ++= List(
        new Anonymisation(spark = spark),
        new WriteToHDFS(spark = spark, storage = Storage.ANONYMISED),
        new InsertIntoHive(spark = spark, storage = Storage.ANONYMISED, mode = SaveMode.Append)
      )
    }

    tasks ++= List(
      new SaveMonitoring(sc = sc, spark = spark),
      new MoveHDFS(fs, fileStatus, Storage.RAW)
    )
  }
}
