/**
  * Owner: LBP.
  * Developer: Aimen SIJOUMI
  * Date: 22/02/2019
  * Update: 22/02/2019
  */

package com.lbp.ingestion.config

/**
  *
  */
object Parameters extends Enumeration {
  val ADMINISTRATION = Value("/coffre_fort/administration/*/metadata/*")
  val DATA_LAKE = Value("/coffre_fort/datalake")
  val INGRESS = Value("/coffre_fort/ingress/copyRawFiles")
}
