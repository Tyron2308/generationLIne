/**
  * Owner: LBP.
  * Developer: Aimen SIJOUMI & Alison RAKOTOMAHEFA
  * Date: 25/01/2019
  * Update: 01/02/2019
  */

package com.lbp.ingestion.workflow

import com.lbp.ingestion.structure.FlowFile
import com.lbp.ingestion.task.Task

/**
  * This trait can be compared to Java Interface. Each task of an ingestion has to extends Workflow.
  */
trait Workflow {

  /**
    * For each workflow, we have an ingestion structure, which is composed with the file, and other variables.
    */
  var flowFile: FlowFile

  /**
    * A workflow has a list of task, this is a linked list, and task will be started one after one.
    */
  var tasks: List[Task]

  /**
    * For a workflow, the run function will be execute the process function of each task in the tasks.
    */
  def run(): Unit = {
    tasks.foreach(t => t.process(flowFile))
  }
}
