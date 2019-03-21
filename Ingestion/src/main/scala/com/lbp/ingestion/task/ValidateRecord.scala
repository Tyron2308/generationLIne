/**
  * Owner: LBP.
  * Developer: Aimen SIJOUMI & Alison RAKOTOMAHEFA & Tyron FERREIRA
  * Date: 25/01/2019
  * Update: 26/02/2019
  */

package com.lbp.ingestion.task

import com.lbp.ingestion.structure._
import org.apache.spark.sql.Row

import scala.annotation.switch

/**
  * Validate all records from an ingestion. This will parse the file and valid lines.
  *
  */
class ValidateRecord extends Task with Serializable {

  private type State = Int
  private final val Start = 0
  private final val Field = 1
  private final val Delimiter = 2
  private final val End = 3
  private final val QuoteStart = 4
  private final val QuoteEnd = 5
  private final val QuotedField = 6

  /**
    * Split the line according to the delimiter.
    *
    * @param line to be splitted.
    * @param delimiter to split the line
    * @return an Array[String] with contains each element splitted by the delimiter.
    */
  def split(line: String, delimiter: String, escapeChar: Char = '"'): Array[String] = {
    line.split(delimiter, -1).foldLeft(List[String]())((acc: List[String], element: String) => {
      if(element.length > 1) {
        if(element.charAt(element.length - 1) == escapeChar) acc.take(acc.length - 1) ::: List[String](acc.last + delimiter + element)
        else acc :+ element
      }
       else acc :+ element
    }).toArray
  }

  def parse(input: String, escapeChar: Char, delimiter: Char, quoteChar: Char): Option[List[String]] = {
    val buf: Array[Char] = input.toCharArray
    var fields: Vector[String] = Vector()
    var field = new StringBuilder
    var state: State = Start
    var pos = 0
    val buflen = buf.length

    if (buf.length > 0 && buf(0) == '\uFEFF') {
      pos += 1
    }

    while (state != End && pos < buflen) {
      val c = buf(pos)
      (state: @switch) match {
        case Start => {
          c match {
            case `quoteChar` => {
              state = QuoteStart
              pos += 1
            }
            case `delimiter` => {
              fields :+= field.toString
              field = new StringBuilder
              state = Delimiter
              pos += 1
            }
            case '\n' | '\u2028' | '\u2029' | '\u0085' => {
              fields :+= field.toString
              field = new StringBuilder
              state = End
              pos += 1
            }
            case '\r' => {
              if (pos + 1 < buflen && buf(1) == '\n') {
                pos += 1
              }
              fields :+= field.toString
              field = new StringBuilder
              state = End
              pos += 1
            }
            case x => {
              field += x
              state = Field
              pos += 1
            }
          }
        }
        case Delimiter => {
          c match {
            case `quoteChar` => {
              state = QuoteStart
              pos += 1
            }
            case `escapeChar` => {
              if (pos + 1 < buflen
                && (buf(pos + 1) == escapeChar || buf(pos + 1) == delimiter)) {
                field += buf(pos + 1)
                state = Field
                pos += 2
              } else {
                throw new Exception(buf.mkString)
              }
            }
            case `delimiter` => {
              fields :+= field.toString
              field = new StringBuilder
              state = Delimiter
              pos += 1
            }
            case '\n' | '\u2028' | '\u2029' | '\u0085' => {
              fields :+= field.toString
              field = new StringBuilder
              state = End
              pos += 1
            }
            case '\r' => {
              if (pos + 1 < buflen && buf(1) == '\n') {
                pos += 1
              }
              fields :+= field.toString
              field = new StringBuilder
              state = End
              pos += 1
            }
            case x => {
              field += x
              state = Field
              pos += 1
            }
          }
        }
        case Field => {
          c match {
            case `escapeChar` => {
              if (pos + 1 < buflen) {
                if (buf(pos + 1) == escapeChar
                  || buf(pos + 1) == delimiter) {
                  field += buf(pos + 1)
                  state = Field
                  pos += 2
                } else {
                  throw new Exception(buf.mkString)
                }
              } else {
                state = QuoteEnd
                pos += 1
              }
            }
            case `delimiter` => {
              fields :+= field.toString
              field = new StringBuilder
              state = Delimiter
              pos += 1
            }
            case '\n' | '\u2028' | '\u2029' | '\u0085' => {
              fields :+= field.toString
              field = new StringBuilder
              state = End
              pos += 1
            }
            case '\r' => {
              if (pos + 1 < buflen && buf(1) == '\n') {
                pos += 1
              }
              fields :+= field.toString
              field = new StringBuilder
              state = End
              pos += 1
            }
            case x => {
              field += x
              state = Field
              pos += 1
            }
          }
        }
        case QuoteStart => {
          c match {
            case `escapeChar` if escapeChar != quoteChar => {
              if (pos + 1 < buflen) {
                if (buf(pos + 1) == escapeChar
                  || buf(pos + 1) == quoteChar) {
                  field += buf(pos + 1)
                  state = QuotedField
                  pos += 2
                } else {
                  throw new Exception(buf.mkString)
                }
              } else {
                throw new Exception(buf.mkString)
              }
            }
            case `quoteChar` => {
              if (pos + 1 < buflen && buf(pos + 1) == quoteChar) {
                field += quoteChar
                state = QuotedField
                pos += 2
              } else {
                state = QuoteEnd
                pos += 1
              }
            }
            case x => {
              field += x
              state = QuotedField
              pos += 1
            }
          }
        }
        case QuoteEnd => {
          c match {
            case `delimiter` => {
              fields :+= field.toString
              field = new StringBuilder
              state = Delimiter
              pos += 1
            }
            case '\n' | '\u2028' | '\u2029' | '\u0085' => {
              fields :+= field.toString
              field = new StringBuilder
              state = End
              pos += 1
            }
            case '\r' => {
              if (pos + 1 < buflen && buf(1) == '\n') {
                pos += 1
              }
              fields :+= field.toString
              field = new StringBuilder
              state = End
              pos += 1
            }
            case _ => {
              throw new Exception(buf.mkString)
            }
          }
        }
        case QuotedField => {
          c match {
            case `escapeChar` if escapeChar != quoteChar => {
              if (pos + 1 < buflen) {
                if (buf(pos + 1) == escapeChar
                  || buf(pos + 1) == quoteChar) {
                  field += buf(pos + 1)
                  state = QuotedField
                  pos += 2
                } else {
                  throw new Exception(buf.mkString)
                }
              } else {
                throw new Exception(buf.mkString)
              }
            }
            case `quoteChar` => {
              if (pos + 1 < buflen && buf(pos + 1) == quoteChar) {
                field += quoteChar
                state = QuotedField
                pos += 2
              } else {
                state = QuoteEnd
                pos += 1
              }
            }
            case x => {
              field += x
              state = QuotedField
              pos += 1
            }
          }
        }
        case End => {
          sys.error("unexpected error")
        }
      }
    }
    (state: @switch) match {
      case Delimiter => {
        fields :+= ""
        Some(fields.toList)
      }
      case QuotedField => {
        None
      }
      case _ => {
        // When no crlf at end of file
        state match {
          case Field | QuoteEnd => {
            fields :+= field.toString
          }
          case _ => {
          }
        }
        Some(fields.toList)
      }
    }
  }

  /**
    * Split the line according to a list of positions.
    *
    * @param line to be splitted.
    * @param positions which represents an the positions to split the line
    * @return an Array[String] with contains each element splitted by the delimiter.
    */
  def split(line: String, positions: List[Position]): Array[String]= {
    positions.map(pos => line.slice(pos.start-1, pos.end)).toArray
  }

  /**
    * Validate a columns with a schema. It will convert each column by the type defined in the schema.
    *
    * @param columns the columns to validate.
    * @param fields the schema.
    * @return a tuple composed by a String and a Row. The string indicates if the line is valid or invalid according to the schema.
    */
  def validateRecord(columns: Array[String], fields: List[Field]): (Storage.Value, Row) = {
    try {
      if(columns.length == fields.size) {
        (Storage.VALID, Row.fromSeq(fields.zipAll(columns, "", "").map{ case (f: Field, s: String) =>
          f.hiveType match {
            case "int" | "long" | "float" | "double" =>
              val cleanedColumn = s.replace(" ", "")
              f.hiveType match {
                case "int" => if(f.nullable && cleanedColumn.isEmpty) null else cleanedColumn.toInt
                case "long" => if(f.nullable && cleanedColumn.isEmpty) null else cleanedColumn.toLong
                case "float" => if(f.nullable && cleanedColumn.isEmpty) null else cleanedColumn.replace(",", ".").toFloat
                case "double" => if(f.nullable && cleanedColumn.isEmpty) null else cleanedColumn.replace(",", ".").toDouble
              }
            case "string" | _ =>
              if(f.nullable && (s.isEmpty || s.equals("null"))) null else s
          }
        }))
      }
      else {
        Logger.log.error(s"[${this.getClass.getSimpleName}] Size not match with schema.")
        (Storage.INVALID, Row.fromSeq(columns))
      }
    }
    catch {
      case e: Exception =>
        Logger.log.error(s"[${this.getClass.getSimpleName}] $e")
        (Storage.INVALID, Row.fromSeq(columns))
    }
  }

  /**
    * @param flowFile represents the data of the current workflow.
    */
  override def process(flowFile: FlowFile): Unit = {

    Logger.log.info(s"[${this.getClass.getSimpleName}] Split and valid records for file: ${flowFile.filename}")

    val schema = flowFile.schema.get

    flowFile.validateRecord = flowFile.rdds(Storage.RAW) match {
      case RDDString(rdd) =>

        val headerLines = schema.header.getOrElse(0L)
        Logger.log.info(s"[${this.getClass.getSimpleName}] Skip header: $headerLines lines")

        val footerLines = schema.footer.getOrElse(0L)
        Logger.log.info(s"[${this.getClass.getSimpleName}] Skip footer: $footerLines lines")

        val filteredRdd = if(headerLines + footerLines == 0) rdd else {

          val footerLinesWithCount = if(footerLines == 0) rdd.count else rdd.count - footerLines

          rdd
            .zipWithIndex
            .map(t => (t._2, t._1))
            .filterByRange(headerLines, footerLinesWithCount)
            .map(t => t._2)
        }

        Some(
          filteredRdd
            .map(line => {
              if(schema.delimiter.isDefined)
                parse(line, '\\', schema.delimiter.get(0), schema.quoteChar.get.charAt(0)).get.toArray
              else
                split(line, schema.positions.get)
            })
            .map(columns =>  validateRecord(columns, schema.fields))
        )
    }
  }
}
