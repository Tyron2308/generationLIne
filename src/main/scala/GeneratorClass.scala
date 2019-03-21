import org.apache.spark.sql.types.DateType
import scala.util.Random
import OBJRandom.RandomField


object EnumMap extends Enumeration {
  type EnumMap = Value
  val VALID, INVALID, WBOOL, TYPEFALSE, MORECOLUMN = Value
}


object OBJRandom {
  implicit class RandomField(l: ClassGenerated) {
    def rand = new Random()
    def printableString = () =>  (for (i <- 0 until 10)
      yield rand.nextPrintableChar())
      .mkString("")
      .filter(_ != '"')
      .filter(_ != '\\')
    def new_rand = () => BigDecimal(rand.nextDouble()).setScale(2, BigDecimal.RoundingMode.HALF_UP)
      .toDouble.toString
    def nextInt = rand.nextInt().toString
    def nextTrueInt = rand.nextInt()
  }
}

final case class Field(name: Option[String], hiveType: String, nullable: Boolean, pivot: Boolean)
abstract class ClassGenerated {
  def generateFalseField(s: List[Field]): List[Field]
}

/*
*  Ajoute une columns a la liste de Field
*  */
class GenerateMoreColumns extends ClassGenerated {
  override def generateFalseField(flow: List[Field]): List[Field] = {

    val falseField = List(Field(Some("falsename"), "double", true, true))
    flow.foldLeft(List[Field]())((acc, elem) => {
      elem.hiveType match {
        case "double" =>
          acc ++ List(Field(Some(this.new_rand()), elem.hiveType, elem.nullable, elem.pivot)) ++ falseField
        case "string" =>
          acc ++ List(Field(Some("\"" + this.printableString()), elem.hiveType, elem.nullable, elem.pivot)) ++ falseField
        case "int" =>
          acc ++ List(Field(Some(this.nextInt), elem.hiveType, elem.nullable, elem.pivot)) ++ falseField
      }
    })}
}

/*
* Genere des faux type pour certains champs
* */
class GenerateFalseType extends ClassGenerated {
  override def generateFalseField(flow: List[Field]): List[Field] = {
    flow.foldLeft(List[Field]())((acc, elem) => {
      elem.hiveType match {
        case "double" =>
          acc ++ List(Field(Some("\"" + this.printableString()), elem.hiveType, elem.nullable, elem.pivot))
        case "string" =>
          acc ++ List(Field(Some(this.new_rand()), elem.hiveType, elem.nullable, elem.pivot))
        case "int" =>
          acc ++ List(Field(Some("\"" + this.printableString()), elem.hiveType, elem.nullable, elem.pivot))
      }
    })}
}

/*
* Genere des champs a null
* */
class GenerateWrongBoolean extends ClassGenerated {
  override def generateFalseField(flow: List[Field]): List[Field] = {

    flow.foldLeft(List[Field]())((acc, elem) => {
      elem.nullable match {
        case true =>
          if (this.nextTrueInt % 2 == 0)
            acc ++ List(Field(None, elem.hiveType, elem.nullable, elem.pivot))
          else
            acc ++ List(Field(Some(this.new_rand()), elem.hiveType, elem.nullable, elem.pivot))
        case false =>
          if (this.nextTrueInt % 2 == 0)
            acc ++ List(Field(None, elem.hiveType, elem.nullable, elem.pivot))
          else
            acc ++ List(Field(Some("\"" + this.printableString()), elem.hiveType, elem.nullable, elem.pivot))
      }
    })}
}

/**
  *
  *
* */
class ValidLine extends ClassGenerated {

  override def generateFalseField(flow: List[Field]): List[Field] = {
    flow.foldLeft(List[Field]())((acc, elem) =>  {
      elem.hiveType match {
        case "double" =>
          acc ++ List(Field(Some(this.new_rand()), elem.hiveType, elem.nullable, elem.pivot))
        case "string" =>
          acc ++ List(Field(Some("\"" + this.printableString() + "\""), elem.hiveType, elem.nullable, elem.pivot))
        case "int" =>
          acc ++ List(Field(Some(this.nextInt), elem.hiveType, elem.nullable, elem.pivot))

      }
    })
  }
}
