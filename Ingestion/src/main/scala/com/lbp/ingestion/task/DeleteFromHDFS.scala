/**
  * Owner: LBP.
  * Developer: Aimen SIJOUMI & Alison RAKOTOMAHEFA
  * Date: 25/01/2019
  * Update: 26/02/2019
  */

package com.lbp.ingestion.task

import com.lbp.ingestion.config.Parameters
import com.lbp.ingestion.structure.{FlowFile, Logger, Storage}
import org.apache.hadoop.fs.{FileSystem, Path}

/**
  * Remove the files at datalakeDirectory for the specific source.
  *
  * @param fs represents the file system (from hadoop).
  * @param from represents the source directory.
  */
class DeleteFromHDFS(fs: FileSystem, from: Parameters.Value) extends Task {

  /**
    * Delete the file to the given path. Delete will be recursive.
    *
    * @param path the path of the file.
    */
  def delete(path: Path): Unit = {
    Logger.log.info(s"[${this.getClass.getSimpleName}] Remove $path")
    this.fs.delete(path, true)
  }

  /**
    * @param flowFile represents the data of the current workflow.
    */
  override def process(flowFile: FlowFile): Unit = {

    val schema = flowFile.schema.get

    from match {
      case Parameters.DATA_LAKE =>
        val datalakeDirectory = s"${Parameters.DATA_LAKE}/${schema.branch}"
        val path = s"${schema.sourceGroup}/${schema.tableName}${if (flowFile.timeStamp.isDefined) s"/${flowFile.timeStamp}" else ""}"

        Storage.values.toSeq
            .foreach(storage => {
              this.delete(new Path(s"$datalakeDirectory/$storage/$path".toLowerCase))
            })
    }
  }
}
