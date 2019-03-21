/**
  * Owner: LBP.
  * Developer: Aimen SIJOUMI & Alison RAKOTOMAHEFA
  * Date: 29/01/2019
  * Update: 26/02/2019
  */

package com.lbp.ingestion.task

import com.lbp.ingestion.structure.{FlowFile, Logger, RDDString, Storage}

/**
  * Extract Timestamp from the header of the file.
  */
class ExtractTimestamp extends Task {

  /**
    * @param flowFile represents the data of the current workflow.
    */
  override def process(flowFile: FlowFile): Unit = {

    Logger.log.info(s"[${this.getClass.getSimpleName}] Extract timestamp for file ${flowFile.filename}")

    flowFile.rdds(Storage.RAW) match {
      case RDDString(rdd) => if (rdd.isEmpty()) {
        val header = rdd.first
        // TODO to define into the schema. A regex will be a good idea !!!!!!!!!!!!
        flowFile.timeStamp = Some(header.slice(16,22))
      }
    }
  }
}
