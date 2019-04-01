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
    fieldLigne.foreach(println(_))
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
                     (pGoodAlter: Double, numIteration: Int, pathRaw: String): Boolean = {
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
    toOutput(lines, pathRaw, pGoodAlter) match {
      case Success(check) =>
        println(s"value return by toOutput $check")
        check
      case Failure(failed) =>
        println(s"value return by toOutput $failed")
        false
    }

  }

  @throws
  private def toOutput(l: List[String], pathRaw: String, p: Double): Try[Boolean] = {
    import java.io._
    import java.time.format.DateTimeFormatter
    import java.time.LocalDateTime

    val filename = _schama.getOrElse(None) match {
      case dataFrame if dataFrame != None =>
        dataFrame.asInstanceOf[sql.DataFrame].select("nameFile")
          .collect()
          .head
          .getString(0)
      case None => throw new RuntimeException("filename is not valid inside dataFrame")
    }

    val conf = new Configuration()
    // conf.set("fs.defaultFS", Generate.hdfsUser)
    val fs: FileSystem = FileSystem.get(conf)
    val timeStamp = DateTimeFormatter.ofPattern("yyyyMMddHHmm").format(LocalDateTime.now)
    val output = fs.create(new Path(pathRaw + filename + "." + p.toString +"-" + (100-p).toString + "." + timeStamp))
    val writer = new PrintWriter(output)
    try {
      l.foreach(element => writer.write(element + "\n"))
      Try(true)
    } catch {
      case e: IOException => {
        println(e.printStackTrace())
        Try(false)
      }
    }
    finally {
      writer.close()
      println(s"file opened $filename, Closed!")
    }
  }
}

object Generate {
  final val field = "fields"
  // final val hdfsUser = "hdfs://192.168.43.16:9000"
}

object GenerateRecords {

  /**
    *
    * @param numberOfLine number of line generate for this batch of line
    * @param p % of good line inside the file
    * @return true or false if function succeed/fail
    */
  def generateValidRecords(spark: SparkSession, numberOfLine: Int, path:String, schemaToRead: String,
                           generator: Generate, p:Double): Boolean = {
    import EnumMap._

    val fieldLine = generator.load(spark, schemaToRead)
    val check = generator.createRawString(fieldLine, Map(VALID -> new ValidLine,
      WBOOL -> new GenerateWrongBoolean,
      MORECOLUMN -> new GenerateMoreColumns,
      TYPEFALSE -> new GenerateFalseType))(p, numberOfLine, path)
    println(s"isCreateRawStringDone $check")
    check
  }

  /*
  * args(0) => nombre de ligne par fichier
  * args(2) => path ou ecrire le fichier sur hdfs
  * args(1) => pourcentage de ligne valide
  * args(3) => schema to load
   */
  def main(args: Array[String]): Unit = {
    require(args.length > 2, "nombre de ligne - path to write file - schema to read ")
    val spark = SparkSession.builder
      .appName("My Spark Application")
      .master("local[*]")
      .config(new SparkConf().setAppName("Line Generator"))
      .getOrCreate
    Logger.getLogger("org").setLevel(Level.ERROR)
    val generator = new Generate()
    for (i <- 3 until args.length)
      generateValidRecords(spark, args(0).toInt, args(2), args(i), generator, args(1).toDouble)
    spark.stop
  }
}

