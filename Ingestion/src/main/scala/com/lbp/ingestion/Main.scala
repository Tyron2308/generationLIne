/**
  * Owner: LBP.
  * Developer: Aimen SIJOUMI & Alison RAKOTOMAHEFA
  * Date: 25/01/2019
  * Update: 26/02/2019
  */

package com.lbp.ingestion

import java.text.SimpleDateFormat
import java.util.Calendar

import com.lbp.ingestion.config.{Config, Parameters}
import com.lbp.ingestion.structure.Logger
import com.lbp.ingestion.workflow.{Append, FullHist, FullTotal}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}
import scopt.OptionParser

object Main {

  /**
    * This function prepare the option parser. Which represents the argument for the ingestion motor.
    *
    * @return the arguments parser.
    */
  def getOptionParser: OptionParser[Config] = {
    new scopt.OptionParser[Config]("ingestion-motor") {
      head("scopt", "3.x")
      opt[String]('a', "app-name").required().action((x, c) => c.copy(appName = x)).text("The app-name of spark job.")
      opt[String]('p', "pattern").required().action((x, c) => c.copy(pattern = x)).text("The pattern of the process files.")
      opt[Int]("anLevel").action((x, c) => c.copy(anLevel = x)).text("Anonymization level.")
      opt[Boolean]('h', "header").action((x, c) => c.copy(header = x)).text("Specifies if the files contains a header.")
      opt[String]('l', "start-with").action((x, c) => c.copy(startLineWith = x)).text("Specifies the first caracters of the lines.")
      opt[String]('m', "mode").required().action((x, c) => c.copy(ingestionMode = x)).validate(x => {
        if (x.equals("FULL_TOTAL") || x.equals("FULL_HIST") || x.equals("APPEND")) success
        else failure("mode must be (FULL_TOTAL|FULL_HIST|APPEND)")
      }).text("Value is FULL_TOTAL|FULL_HIST|APPEND")
      opt[Boolean]('d', "distinct").action((x, c) => c.copy(distinct = x)).text("Specifies if lines have to be distinct.")
    }
  }

  /**
    * The main function of this object.
    *
    * @param args the list of args passed into the program
    */
  def main(args: Array[String]): Unit = {

    println("=======>")
    args.map(println(_))
    getOptionParser.parse(args, Config()) match {
      case Some(config) =>
        val conf = new Configuration()
          conf.addResource(new Path("/usr/local/Cellar/hadoop-2.7.4/etc/hadoop/core-site.xml"))
          conf.addResource(new Path("/usr/local/Cellar/hadoop-2.7.4/etc/hadoop/hdfs-site.xml"))
        val fs: FileSystem = FileSystem.get(conf)
        val sc: SparkContext = new SparkContext(new SparkConf().setAppName(config.appName).setMaster("local[*]"))
        val spark: SparkSession = SparkSession.builder.appName(config.appName).getOrCreate()

        val format = new SimpleDateFormat("dd/MM/yyyy HH:mm:ss")
        Logger.log.info("==================================================")
        Logger.log.info(s"==== STARTING INGESTION: ${format.format(Calendar.getInstance().getTime)} =====")
        Logger.log.info("==================================================")

        fs.listStatus(new Path(Parameters.INGRESS.toString))
          .filter(f => f.getPath.toString.split("/").last.matches(config.pattern))
          .foreach(fileStatus => {
            try {
              val regex = config.pattern.r
              val filename = fileStatus.toString.split("/").last match {
                case regex(filename) => filename
              }
              config.ingestionMode match {
                case "FULL_TOTAL" => new FullTotal(fs, fileStatus, sc, spark, filename, config).run()
                case "FULL_HIST" => new FullHist(fs, fileStatus, sc, spark, filename, config).run()
                case "APPEND" => new Append(fs, fileStatus, sc, spark, filename, config).run()
              }
            }
            catch {
              case e: Exception => Logger.log.fatal(e.toString)
            }
          })

        Logger.log.info("==================================================")
        Logger.log.info("================ END OF INGESTION ================")
        Logger.log.info("==================================================")

      case _ =>
        Logger.log.fatal("Something strange with parameters.")
        System.exit(-1)
    }
  }
}
