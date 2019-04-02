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
  /*
  * on stoque le shéma dans une Option pour une meilleure gestion des erreurs.
  * On l'utiliser pour récupére les différentes options souhaitées dans le shchéma load.
  * */
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
      for(_ <- 0 until totalIteration)
        yield Some(caller(VALID).generateFalseField(as))
  }

  /**
    *
    * pour pouvoir générer des erreurs aléatoire on passse par une méthode privée,
    * qu'on appelle en parametre (call by Name, enum: => retour) de génerateString.
    */
  private def randomError(): EnumMap.Value = {
    val A = Array.range(1, 4)
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
    * generate raw string creer un batch de ligne valide et invalide en fonction du pourcentage souhaite.
    * @param as List field fetch from schema stored in ressource directory
    * @param AlterJobs Functor who create line
    * @param pGoodAlter pourcentage of good line in this batch
    * @param numIteration number of line wanted in this batch
    *
    *
    *  rawlineVector est une listBuffer qui s'occupe d'append les lignes créer via les classes ClassGenerated.
    *  On récupere les élements souhaitées dans la liste de Field, puis on s'occupe de transformer cette liste en string sous format CSV
    *  pour pouvoir stocker celles-ci dans un fichier HDFS.
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
    val delimiter = _schama.get.select("delimiter").first.toSeq.head.toString
    val lines = rawLineVector.map(_.getOrElse(List()))
      .map(element => element.map(_.name.getOrElse("")).mkString(delimiter))
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

  /**
    *
    * @param l list de String a écrire dans un fichier.
    * @param pathRaw path du directory ou ouvrir le fichier
    * @param p pourcentage de ligne valide
    *
    * fonction qui peux throw des erreurs et retourne un Success en cas de réussite. (si l'on souhaite faire de l'asynchrone par la suite) ou bien Failure.
    *
    */
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

  def main(args: Array[String]): Unit = {
    require(args.length > 2, "nombre de ligne - path to write file - schema to read ")
    val spark = SparkSession.builder
      .appName("My Spark Application")
      .master("local[*]")
      .config(new SparkConf().setAppName("Line Generator"))
      .getOrCreate
    Logger.getLogger("org").setLevel(Level.ERROR)

    val generator   = new Generate()
    val numberLine  = ParsingArgument.isNumberLineValid(args(0))
    val pValidLine  = ParsingArgument.isPValid(ParsingArgument.isNumberLineValid(args(1)).toDouble)
    val validPath   = ParsingArgument.isPathValid(args(2))

    (numberLine, pValidLine, validPath.getOrElse("")) match {
      case (-1, _, _) => println(s" numberLine is invalid $numberLine")
      case (_, _, "") => println(s" path is not valid $validPath")
      case (lines, p, path) =>
        for (i <- 3 until args.length)
        ParsingArgument.isDirectory(args(i), generateValidRecords)(spark, lines, path, generator, p)
    }
    spark.stop
  }
}

