import java.io.FileSystem

import org.apache.hadoop.conf.Configuration
import org.apache.spark.sql.SparkSession

object ParsingArgument {

  def isNumberLineValid(numberLines: String): Int = numberLines.toInt match  {
    case validNumber => validNumber
    case Exception => -1
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
    case _ => _
  }

  /*
  *  isDirectory gere le cas ou l'on souhaite passer un directory en argument du programme ou bien,
  *  une liste de path pour une source en particulier.
   *  Cette fonction prend une autre fonction en parametre
   *  afin de pouvoir encapsuler la gestion des possibilitées laisser a l'utilisateur final
  * */
  def isDirectory(path: String,
                  f: (SparkSession, Int, String, String, Generate, Double) => Boolean)
                 (s: SparkSession, nLine: Int, storePath: String, generator: Generate, p: Double): Boolean = {
    import java.io.File

    val d = new File(path)
    if (d.exists && d.isDirectory) {
      d.list().foreach(filename => {
          f(s, nLine, storePath, filename, generator, p)
      })
    }
    f(s, nLine, storePath, path, generator, p)
  }
}
