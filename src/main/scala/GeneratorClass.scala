import org.apache.spark.sql.types.DateType
import scala.util.Random
import OBJRandom.RandomField

object EnumMap extends Enumeration {
  type EnumMap = Value
  val VALID, INVALID, WBOOL, TYPEFALSE, MORECOLUMN = Value
}

/**
  * OBJRandom est l'objet qui permet de regrouper toute les fonctions randoms. cette objet est implicit, c'est a dire qu'il n'a pas besoin d'etre defini quelque part.
  * En effet celui-ci ajoute des methodes au type ClassGenerated et c'est donc pourquoi on peux faire this.printable() ou this.new_rand depuis un objet
  * qui etenderais ClassGenerated
  */
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

/*
* case class field, objet defini au moment ou l'on load un schema avec la methode Generate.load.
* @name est une option car c'est un champ qui peux devenir none en cas de generation de mauvaise ligne.
* */
final case class Field(name: Option[String], hiveType: String, nullable: Boolean, pivot: Boolean)

/**
  * ClassGenerated est la classe abstraite mere qui permet de mettre en place une abstraction simple. Afin de pouvoir définir
  * un mauvais comportement lors de la création d'une ligne de maniere générique on utilise la possibilité d'override la méthode generateFalseField(l:field)
  * afin d'implémenter le comportement voulu et que celui-ci s'intégre au reste des différents comportements (GenerateMoreColumns, ValidLigne..)
  *
  * Chaque classe qui etend la classe mere ClassGenerated se voit attribuer le role de créer une ligne d'un fichier en entier. Ainsi on peux modeler un fichier
  * comme on le souhaite
  */
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
  * Creer une ligne valide
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
