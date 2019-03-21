/**
  * Owner: LBP.
  * Developer: Aimen SIJOUMI & Alison RAKOTOMAHEFA
  * Date: 25/01/2019
  * Update: 26/02/2019
  */

package com.lbp.ingestion.structure

/**
  * The schema for a specific FlowFile.
  *
  * @param tableName the table name for the specific FlowFile.
  * @param fieldsNumber the number of fields in the file.
  * @param branch the branch of the file.
  * @param sourceGroup the source of the file.
  * @param fields the fields which represents the columns.
  * @param delimiter the delimiter of the lines.
  * @param header a boolean indicating if the file contains a header.
  * @param footer  a boolean indicating if the file contains a footer.
  * @param ddl the DDL if new table has to be created.
  * @param query the anonymization query.
  * @param positions the positions for lines delimited by fixed positions.
  */
final case class FlowFileSchema(
                                 tableName: String,
                                 fieldsNumber: Long,
                                 branch: String,
                                 sourceGroup: String,
                                 fields: List[Field],
                                 delimiter: Option[String],
                                 quoteChar: Option[String],
                                 header: Option[Long],
                                 footer: Option[Long],
                                 ddl: Option[String],
                                 query: Option[String],
                                 positions: Option[List[Position]]
                               ) extends Serializable
