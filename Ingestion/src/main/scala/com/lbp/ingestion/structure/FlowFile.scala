/**
  * Owner: LBP.
  * Developer: Aimen SIJOUMI & Alison RAKOTOMAHEFA
  * Date: 25/01/2019
  * Update: 26/02/2019
  */

package com.lbp.ingestion.structure

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.StructType

/**
  * A FlowFile represents a structure for a workflow. It will be passed to each task to modify itself.
  *
  * @param singleFilename represents the filename without timestamp.
  * @param filename to be processed.
  * @param rdds it's a map of Storage.Value and an RDDType.
  * @param dfSchema the DataFrame schema for the file.
  * @param header a boolean which indicates if there is a header.
  * @param startWith a filter to process only file start with this argument.
  * @param validateRecord RDD of parsed lines.
  * @param anonymisationLevel represents the level anonymization, 0 for no anonymization and 1 the level 1.
  * @param timeStamp timestamp of the FlowFile.
  */
class FlowFile(
                val singleFilename: String,
                val filename: String,
                var rdds: Map[Storage.Value, RDDType],
                var schema: Option[FlowFileSchema] = None,
                var dfSchema: Option[StructType] = None,
                var header: Boolean,
                var startWith: String,
                var validateRecord: Option[RDD[(Storage.Value, Row)]] = None,
                var anonymisationLevel: Int,
                var timeStamp: Option[String] = None
                ) extends Serializable