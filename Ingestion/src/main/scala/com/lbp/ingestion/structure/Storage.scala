/**
  * Owner: LBP.
  * Developer: Aimen SIJOUMI
  * Date: 22/02/2019
  * Update: 26/02/2019
  */

package com.lbp.ingestion.structure

/**
  *
  */
object Storage extends Enumeration {
  val RAW = Value("raw")
  val VALID = Value("an0")
  val INVALID = Value("invalid")
  val ANONYMISED = Value("an1")
}
