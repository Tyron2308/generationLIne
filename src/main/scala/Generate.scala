import org.apache.hadoop.conf.Configuration
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.log4j._
import org.apache.hadoop.fs.{FileSystem, Path}

import scala.collection.mutable.ListBuffer
import scala.util.{Failure, Random, Success, Try}


class Generate {
  import EnumMap._
  type GeneratorCombinator = Map[EnumMap.EnumMap, ClassGenerated]
  var _schama: Option[sql.DataFrame] = None

  /**
    *
    *  This function create a sequence of new valid line/invalid line.
    *  It depend only of the parameter  passed at the function
    *  EnumMap (VALID/INVALID)
    *  caller is the object who handle the generation of a false/valid line.
    *
    * @param totalIteration
    * @param enum
    * @param as
    * @param caller
    * @param enumMap
    * @return
    */
  private def generateString(totalIteration: Int, enum: => EnumMap.Value)
                            (as: List[Field], caller: GeneratorCombinator, enumMap: EnumMap.EnumMap)
  : IndexedSeq[Option[List[Field]]] = {
    if (enumMap == INVALID)
      for(_ <- 0 until totalIteration)
        yield Some(caller(enum).generateFalseField(as))
    else
      for(_ <- 0 until totalIteration - 1)
        yield Some(caller(VALID).generateFalseField(as))
  }

  private def randomError(): EnumMap.Value = {
    val A = Array.range(1, 3)
    Random.shuffle(A.toList).head match {
      case 1 =>  WBOOL
      case 2 =>  TYPEFALSE
      case 3 =>  MORECOLUMN
    }
  }

  /**
    * load a schema from resource directory, parse field and return List[Field] from it
    * @param spark
    * @param path
    * @return
    */

  def load(spark: SparkSession, path: String): Array[Field] = {
    val schema = spark.read.option("multiLine", "true").json(path)

    _schama = Some(schema)
    schema.printSchema()
    val s = schema.select(Generate.field).collect().flatMap(_.toSeq)
    val splited = s.flatMap(elm => { elm.toString().split("]") })
    val trimmed = splited.map(elem => {
      if (elem.length > 1) elem.substring(elem.indexOf("[") + 1) else ""
    }).filter(elem => !elem.isEmpty)

    val fieldLigne = trimmed.map(elem => {
      val array = elem.split(",")
      Field(Some(array(1)), array(0), array(2).toBoolean, array(3).toBoolean)
    })
    fieldLigne
  }


  /**
    * generate raw string
    * @param as List field fetch from schema stored in ressource directory
    * @param AlterJobs Functor who create line
    * @param pGoodAlter pourcentage of good line in this batch
    * @param numIteration number of line wanted in this batch
    * @return
    */
  def createRawString(as: Array[Field], AlterJobs: GeneratorCombinator)
                     (pGoodAlter: Double, numIteration: Int): Boolean = {
    val numIterationEpoch = ((100.0 - pGoodAlter) / 10).toInt
    val rawLineVector = ListBuffer[Option[List[Field]]]()
    while (rawLineVector.length < numIteration - 1) {
        generateString(10 - numIterationEpoch, randomError())(as.toList, AlterJobs, VALID)
          .map(rawLineVector += _)
        generateString(numIterationEpoch, randomError())(as.toList, AlterJobs, INVALID)
          .map(rawLineVector += _)
    }
    val lines = rawLineVector.map(_.getOrElse(List()))
                 .map(element => element.map(_.name.getOrElse("")).mkString(","))
                 .toList

    println("toOutput")
    lines.foreach(println(_))
    toOutput(lines) match {
      case Success(check) =>
        true
      case Failure(failed) =>
        false
    }
  }

  @throws
  private def toOutput(l: List[String]): Try[Boolean] = {
    import java.io._

    val filename = _schama.getOrElse(None) match {
      case dataFrame if dataFrame != None =>
        dataFrame.asInstanceOf[sql.DataFrame].select("nameFile")
                                             .collect()
                                             .head
                                             .getString(0)
      case None => throw new RuntimeException("filename is not valid inside dataFrame")
    }
    println("open file")
    val file = new File(Generate.path + filename)
    val conf = new Configuration()
    conf.set("fs.defaultFS", Generate.hdfsUser)
    val fs: FileSystem = FileSystem.get(conf)
    val output = fs.create(new Path(Generate.pathRaw + filename + ".201808021213519"))
    val writer = new PrintWriter(output)
    val bw = new BufferedWriter(new FileWriter(file))
    try {
      l.foreach(element => writer.write(element + "\n"))
      l.foreach(element => bw.write(element + "\n"))
      Try(true)
    } catch {
      case e: IOException => {
        println(e.printStackTrace())
        Try(false)
      }
    }
    finally {
        writer.close()
        bw.close()
        println(s"file opened $filename, Closed!")
      }
  }
}

object Generate {
  final val pathRaw = "/coffre_fort/ingress/copyRawFiles/"
  final val field = "fields"
  final val path = "src/main/ressource/"
  final val StringSchema = "src/main/ressource/schema"
  final val IntSchema = "src/main/ressource/test2.schema"
  final val hdfsUser = "hdfs://192.168.43.16:9000"
}

object GenerateRecords {

 /**
    *
    * @param numberOfLine number of line generate for this batch of line
    * @param p % of good line inside the file
    * @return true or false if function succeed/fail
    */
  def generateValidRecords(numberOfLine: Int, p: Double = 100.0): Boolean = {
    import EnumMap._
    val spark = SparkSession.builder
      .appName("My Spark Application")
      .master("local[*]")
      .config(new SparkConf().setAppName("Line Generator").setMaster("local"))
      .getOrCreate
    Logger.getLogger("org").setLevel(Level.ERROR)
    val generator = new Generate()
    val fieldLine = generator.load(spark, Generate.IntSchema)
    val check = generator.createRawString(fieldLine, Map(VALID -> new ValidLine,
      WBOOL -> new GenerateWrongBoolean,
      MORECOLUMN -> new GenerateMoreColumns,
      TYPEFALSE -> new GenerateFalseType))(p, numberOfLine)
    println(s"isCreateRawStringDone $check")
    spark.stop
    check
  }

  def generateInvalidRecords(numberOfline: Int,  p: Double): Boolean = {
    generateValidRecords(numberOfline, p)
  }


  def main(args: Array[String]): Unit = {
        generateValidRecords(100)
    }
}


