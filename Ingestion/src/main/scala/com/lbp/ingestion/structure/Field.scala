/**
  * Owner: LBP.
  * Developer: Aimen SIJOUMI
  * Date: 22/02/2019
  * Update: 26/02/2019
  */

package com.lbp.ingestion.structure

/**
  *
  * @param name the name of the field.
  * @param hiveType the hive type of the field.
  * @param nullable indicates if the field is nullable.
  */
case class Field(name: String, hiveType: String, nullable: Boolean, pivot: String)
