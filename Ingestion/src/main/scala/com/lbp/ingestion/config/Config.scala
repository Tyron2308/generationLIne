/**
  * Owner: LBP.
  * Developer: Aimen SIJOUMI & Alison RAKOTOMAHEFA
  * Date: 01/02/2019
  * Update: 01/02/2019
  */

package com.lbp.ingestion.config

/** Config represents the parameters to pass to the main, it contains multiple arguments as follow.
  *
  * @param appName the spark app name.
  * @param pattern the prefix file to be processed.
  * @param header a boolean which indicates if the file had a header.
  * @param startLineWith depends on file to be processed, if the ingestion has to process only lines start with the "startLineWith"
  * @param anLevel represents the anonymization level, 0 for no anonymization and 1 the level 1.
  * @param ingestionMode represents the type of ingestion, values have to be: FULL_TOTAL, FULL_HIST or APPEND
  */
case class Config(
                   appName: String = null,
                   pattern: String = null,
                   header: Boolean = false,
                   startLineWith: String = null,
                   anLevel: Int = 0,
                   ingestionMode: String = null,
                   distinct: Boolean = false
                 )