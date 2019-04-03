import Generate.GeneratorCombinator
import org.apache.hadoop.conf.Configuration
import org.apache.spark.sql.SparkSession

object ParsingArgument {
  type Functor = (SparkSession, Int, String, String, Generate, Double, GeneratorCombinator) => Boolean

  def isNumberLineValid(numberLines: String): Int = {
    try {
      numberLines.toInt
    }
    catch {
      case _: Exception => -1
    }
  }

  def isPathValid(path: String): Option[String] = {
    import org.apache.hadoop.fs.{FileSystem, Path}
    val conf = new Configuration()
    val fs: FileSystem = FileSystem.get(conf)

    fs.exists(new Path(path)) match {
      case true => Some(path)
      case false => None
    }
  }

  def isPValid(p: Double): Double = p match {
    case p if p > 100.0 => 100.0
    case p if p < 0.0 => 100.0
    case p => p
  }

  def isErrorDefined(error: String): List[EnumMap.Value] = {
    def matching(x: String) = x match {
      case "MORECOLUMNS" => EnumMap.MORECOLUMN
      case "TYPEFALSE" => EnumMap.TYPEFALSE
      case "WBOOL" => EnumMap.WBOOL
    }
    error.split(",").toList.map(matching(_))
  }
  /*
  *  isDirectory gere le cas ou l'on souhaite passer un dossier en argument du programme
   * ou bien une liste de path pour une source en particulier.
   * Cette fonction prend une autre fonction en parametre afin de pouvoir encapsuler la gestion des possibilitées laisser a l'utilisateur final
   *
  * */

  def isDirectory(path: String, f: Functor)(s: SparkSession, nLine: Int, storePath: String, generator: Generate,
                                            p: Double, l: GeneratorCombinator): Boolean = {
    import org.apache.hadoop.fs.{FileSystem, Path}
    val conf = new Configuration()
    val fs: FileSystem = FileSystem.get(conf)

    val fsPath = new Path(path)
    if (fs.exists(fsPath) && fs.isDirectory(fsPath)) {
      fs.listStatus(fsPath).foreach(fileStatus => {
        println(s"=====================> nouveau fichier " + fileStatus.getPath.toString)
        f(s, nLine, storePath,  fileStatus.getPath.toString, generator, p)
      })
      true
    }
    else f(s, nLine, storePath, path, generator, p)
  }
}