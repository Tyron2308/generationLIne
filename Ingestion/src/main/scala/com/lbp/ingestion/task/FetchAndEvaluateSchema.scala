/**
  * Owner: LBP.
  * Developer: Aimen SIJOUMI & Alison RAKOTOMAHEFA & Tyron FERREIRA
  * Date: 25/01/2019
  * Update: 26/02/2019
  */

package com.lbp.ingestion.task

import com.lbp.ingestion.config.Parameters
import com.lbp.ingestion.structure.{Field, FlowFile, FlowFileSchema, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.types._

/**
  * Load the schema from a schema file. This file is a JSON file and contains a Avro Schema.
  *
  * @param spark the sparkSession of this job.
  */
class FetchAndEvaluateSchema(spark: SparkSession) extends Task {

  /*val SchemaJson = StructType(List(
    StructField("tableName", StringType, nullable = false),
    StructField("fieldsNumber", LongType, nullable = false),
    StructField("branch", StringType, nullable = false),
    StructField("sourceGroup", StringType, nullable = false),
    StructField("fields", ArrayType(StructType(List(
      StructField("name", StringType, nullable = false),
      StructField("hiveType", StringType, nullable = false),
      StructField("nullable", BooleanType, nullable = false),
      StructField("pivot", StringType, nullable = false)
    ))), nullable = false),
    StructField("delimiter", StringType, nullable = true),
    StructField("quoteChar", StringType, nullable = true),
    StructField("header", LongType, nullable = true),
    StructField("footer", LongType, nullable = true),
    StructField("ddl", StringType, nullable = true),
    StructField("query", StringType, nullable = true),
    StructField("positions", ArrayType(StructType(List(
      StructField("name", StringType, nullable = false),
      StructField("start", IntegerType, nullable = false),
      StructField("end", IntegerType, nullable = false)
    ))), nullable = true)
  ))*/

  /**
    * @param flowFile represents the data of the current workflow.
    */
  override def process(flowFile: FlowFile): Unit = {

    Logger.log.info(s"[${this.getClass.getSimpleName}] Evaluate schema for file ${flowFile.filename}")

    import spark.implicits._

    val schema = spark.read.option("multiLine", "true")
      .schema(ScalaReflection.schemaFor[FlowFileSchema].dataType.asInstanceOf[StructType])
      .json(s"${Parameters.ADMINISTRATION}/${flowFile.singleFilename}.schema").as[FlowFileSchema].first

    schema.fields.foreach((field: Field) => println(field.hiveType))

    flowFile.schema = Some(schema)
    flowFile.dfSchema = Some(StructType(schema.fields.map(field => field.hiveType match {
      case "int" => StructField(field.name, IntegerType, nullable = field.nullable)
      case "long" => StructField(field.name, LongType, nullable = field.nullable)
      case "float" => StructField(field.name, FloatType, nullable = field.nullable)
      case "double" => StructField(field.name, DoubleType, nullable = field.nullable)
      case "string" => StructField(field.name, StringType, nullable = field.nullable)
      case _ => StructField(field.name, StringType, nullable = field.nullable)
    })))
  }
}
