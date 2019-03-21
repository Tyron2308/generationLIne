package com.lbp.ingestion.task
import com.lbp.ingestion.config.Parameters
import com.lbp.ingestion.structure.{FlowFile, Logger, Storage}
import org.apache.hadoop.fs.{FileStatus, FileSystem, Path}

class MoveHDFS(fs: FileSystem, fileStatus: FileStatus, storage: Storage.Value) extends Task {

  /**
    * A task has to overwrite the proces function.
    * In the function, the main of the current task will be called.
    *
    * @param flowFile represents the data of the current workflow.
    */
  override def process(flowFile: FlowFile): Unit = {

    storage match {
      case Storage.RAW =>
        val schema = flowFile.schema.get
        val datalakeDirectory = s"${Parameters.DATA_LAKE}/${schema.branch}"
        val path = s"${schema.sourceGroup}/${schema.tableName.toLowerCase}${if(flowFile.timeStamp.isDefined) s"/${flowFile.timeStamp.get}" else ""}"

        val fromPath = new Path(s"${Parameters.INGRESS}/${flowFile.filename}")

        val destPath = new Path(s"$datalakeDirectory/${Storage.RAW}/$path")
        if(!fs.isDirectory(destPath))
          fs.mkdirs(destPath)

        Logger.log.info(s"[${this.getClass.getSimpleName}] Move file from $fromPath to $datalakeDirectory/${Storage.RAW}/$path")

        fs.rename(
          fromPath,
          destPath
        )
    }
  }
}
