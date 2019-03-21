/**
  * Owner: LBP.
  * Developer: Aimen SIJOUMI & Alison RAKOTOMAHEFA
  * Date: 25/01/2019
  * Update: 01/02/2019
  */

package com.lbp.ingestion.task

import com.lbp.ingestion.structure.FlowFile

/**
  * This trait can be compared to Java Interface. Each Task of an ingestion has to extends Task.
  */
trait Task {

  /**
    * A task has to overwrite the proces function.
    * In the function, the main of the current task will be called.
    *
    * @param flowFile represents the data of the current workflow.
    */
  def process(flowFile: FlowFile): Unit
}
