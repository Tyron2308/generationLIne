/**
  * Owner: LBP.
  * Developer: Aimen SIJOUMI & Alison RAKOTOMAHEFA
  * Date: 14/02/2019
  * Update: 14/02/2019
  */

package com.lbp.ingestion.structure

/**
  * Position represent the index of start and end of a field.
  *
  * @param name field name.
  * @param start the beginning of the field (human).
  * @param end the end of the field (human).
  */
case class Position(name: String, start: Int, end: Int)