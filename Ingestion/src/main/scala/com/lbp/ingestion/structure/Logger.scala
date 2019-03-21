/**
  * Owner: LBP.
  * Developer: Aimen SIJOUMI & Alison RAKOTOMAHEFA
  * Date: 31/01/2019
  * Update: 01/02/2019
  */

package com.lbp.ingestion.structure

import java.text.SimpleDateFormat
import java.util.Calendar

import org.apache.log4j.{FileAppender, Level, PatternLayout}

/**
  * The logger for the ingestion.
  */
object Logger extends Serializable {
  @transient lazy val log: org.apache.log4j.Logger = org.apache.log4j.Logger.getLogger(getClass.getName)
  val fileAppender = new FileAppender()
  fileAppender.setName("FileLogger")
  fileAppender.setFile(s"${scala.util.Properties.envOrElse("INGESTION", "motor" )}-log/${new SimpleDateFormat("yyyyMMdd-HHmm").format(Calendar.getInstance().getTime)}/ingestion.log")
  fileAppender.setLayout(new PatternLayout("%d - %p - %m%n"))
  fileAppender.setAppend(true)
  fileAppender.activateOptions()

  log.setLevel(Level.INFO)
  log.addAppender(fileAppender)
}

