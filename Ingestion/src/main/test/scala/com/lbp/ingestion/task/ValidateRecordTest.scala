package com.lbp.ingestion.task

import com.lbp.ingestion.structure.{Field, Position, RDDRow, Storage}
import org.scalatest.mockito.MockitoSugar
import org.scalatest.{BeforeAndAfterAll, FunSuite}

import scala.io.Source

class ValidateRecordTest extends FunSuite {

  test("testSplit with delimiter and escapeChar") {

  }

  test("testSplit with positions") {

    val line = Source.fromURL(getClass.getResource("/ValidateRecordTest.txt")).getLines().toArray.head
    val positions = List(
      Position("ZNOPA",1,2),
      Position("NUMFAC",3,18),
      Position("IDEOPT",19,26),
      Position("TYPFCT",27,27),
      Position("NUMCPS",28,47),
      Position("CCP2",48,67),
      Position("DATLDF",68,75),
      Position("DATFAC-2",76,83),
      Position("DATFAC",84,91),
      Position("DATOPE",92,99),
      Position("MTCLTC-T",100,113),
      Position("MTCLHT-T",115,128),
      Position("MTDCTC-T",130,143),
      Position("MTDCHT-T",145,158),
      Position("TAUTAX-1",160,165),
      Position("TYPEIMP",167,167),
      Position("MODIMP",168,168),
      Position("NBREVE",169,176),
      Position("CO",178,179),
      Position("CCT",180,181),
      Position("CCL",182,183),
      Position("PAQSEQ",184,193),
      Position("NBRLIB",194,194),
      Position("LIB 1",195,226),
      Position("LIB 2",227,258),
      Position("LIB 3",259,290),
      Position("LIB 4",291,322),
      Position("ROUE",323,358),
      Position("DATPRES",359,366),
      Position("DATFREC",367,374),
      Position("NBPRES",375,375),
      Position("ETAT",377,384),
      Position("DATEMAJE",385,392),
      Position("DATEIMP",393,400),
      Position("TYCPTIMP",401,401),
      Position("CUGSFP",402,407),
      Position("NUMFACP",408,427),
      Position("NUMFACOR",428,447),
      Position("IDECOM",448,455),
      Position("LBLCRT",456,475),
      Position("CUGFRA",476,478),
      Position("NOPROI",479,514),
      Position("ZCRIM",515,516),
      Position("MFALT2",517,530),
      Position("MFALH2",532,545),
      Position("MFALV1",547,560),
      Position("ETAOPF",562,568),
      Position("DATIMI",570,577),
      Position("FILLER",578,650)
    )

    val expectedResult = Array(
      "  ",
      "                ",
      "TOPA0001",
      "0",
      "0000535L016         ",
      "                    ",
      "20190206",
      "        ",
      "20190204",
      "20190204",
      "00000000000111",
      "00000000000111",
      "00000000000111",
      "00000000000111",
      "000000",
      "D",
      "O",
      "00000000",
      "  ",
      "  ",
      "  ",
      "          ",
      "0",
      "                                ",
      "                                ",
      "                                ",
      "                                ",
      "                                    ",
      "20190206",
      "        ",
      "0",
      "IMPUTE  ",
      "20190206",
      "20190204",
      "0",
      "973880",
      "2019020400003045    ",
      "                    ",
      "COPA0001",
      "Cotis. FC           ",
      "C16",
      "                                    ",
      "OK",
      "00000000000111",
      "00000000000111",
      "00000000000000"
    )
    val result = new ValidateRecord().split(line, positions)

    (result zip expectedResult).foreach {case (r: String, e: String) => {
      assert(r.equals(e))
    }}
  }

  test("validateRecord") {

    val lines = Source.fromURL(getClass.getResource("/ValidateRecordTest.txt")).getLines().toArray
    val fields = List(
      Field("ZNOPA","string",true),
      Field("NUMFAC","string",true),
      Field("IDEOPT","string",false),
      Field("TYPFCT","string",false),
      Field("NUMCPS","string",false),
      Field("CCP2","string",true),
      Field("DATLDF","string",true),
      Field("DATFAC-2","string",true),
      Field("DATFAC","string",false),
      Field("DATOPE","string",false),
      Field("MTCLTC-T","int",false),
      Field("MTCLHT-T","int",false),
      Field("MTDCTC-T","int",false),
      Field("MTDCHT-T","int",false),
      Field("TAUTAX-1","int",false),
      Field("TYPEIMP","string",false),
      Field("MODIMP","string",true),
      Field("NBREVE","int",true),
      Field("CO","int",true),
      Field("CCT","int",true),
      Field("CCL","int",true),
      Field("PAQSEQ","double",true),
      Field("NBRLIB","int",true),
      Field("LIB 1","string",true),
      Field("LIB 2","string",true),
      Field("LIB 3","string",true),
      Field("LIB 4","string",true),
      Field("ROUE","double",true),
      Field("DATPRES","string",true),
      Field("DATFREC","string",true),
      Field("NBPRES","string",true),
      Field("ETAT","string",false),
      Field("DATEMAJE","string",true),
      Field("DATEIMP","string",false),
      Field("TYCPTIMP","string",true),
      Field("CUGSFP","string",false),
      Field("NUMFACP","string",false),
      Field("NUMFACOR","string",false),
      Field("IDECOM","string",true),
      Field("LBLCRT","string",false),
      Field("CUGFRA","string",false),
      Field("NOPROI","string",false),
      Field("ZCRIM","string",true),
      Field("MFALT2","int",true),
      Field("MFALH2","int",true),
      Field("MFALV1","int",true),
      Field("ETAOPF","string",true),
      Field("DATIMI","string",true),
      Field("FILLER","string",true),

    )

    val records = new ValidateRecord().validateRecord(lines, fields)

    // val valid = records.filter(line => line == Storage.VALID)
    // val invalid = records.filter
  }


  /*test("testSplit with positions") {

  }

  test("testValidateRecord less fields") {
    val schema = List(
      Map("name" -> "field1", "type" -> "int"),
      Map("name" -> "field1", "type" -> "string"),
      Map("name" -> "field1", "type" -> List("null", "int")),
      Map("name" -> "field1", "type" -> "float"),
      Map("name" -> "field1", "type" -> List("null", "string"))
    )

    val data = Array("10", "toto", "", "30")
    assert(new ValidateRecord().validateRecord(data, schema)._1.equals("invalid"))
  }

  test("testValidateRecord more fields") {
    val schema = List(
      Map("name" -> "field1", "type" -> "int"),
      Map("name" -> "field1", "type" -> "string"),
      Map("name" -> "field1", "type" -> List("null", "int")),
      Map("name" -> "field1", "type" -> "float"),
      Map("name" -> "field1", "type" -> List("null", "string"))
    )

    val data = Array("10", "toto", "", "30", "", "")
    assert(new ValidateRecord().validateRecord(data, schema)._1.equals("invalid"))
  }

  test("testValidateRecord nullability") {
    val schema = List(
      Map("name" -> "field1", "type" -> "int"),
      Map("name" -> "field1", "type" -> "string"),
      Map("name" -> "field1", "type" -> List("null", "int")),
      Map("name" -> "field1", "type" -> "float"),
      Map("name" -> "field1", "type" -> List("null", "string"))
    )

    val data = Array("10", "", "", "30", "")
    assert(new ValidateRecord().validateRecord(data, schema)._1.equals("invalid"))
  }

  test("testValidateRecord type") {
    val schema = List(
      Map("name" -> "field1", "type" -> "int"),
      Map("name" -> "field1", "type" -> "string"),
      Map("name" -> "field1", "type" -> List("null", "int")),
      Map("name" -> "field1", "type" -> "float"),
      Map("name" -> "field1", "type" -> List("null", "string"))
    )

    val data = Array("10", "", "", "30", "")
    assert(new ValidateRecord().validateRecord(data, schema)._1.equals("invalid"))
  }

  test("testValidateRecord valid") {
    val schema = List(
      Map("name" -> "field1", "type" -> "int"),
      Map("name" -> "field1", "type" -> "string"),
      Map("name" -> "field1", "type" -> List("null", "int")),
      Map("name" -> "field1", "type" -> "float"),
      Map("name" -> "field1", "type" -> List("null", "string"))
    )

    val data = Array("10", "toto", "", "30", "")
    assert(new ValidateRecord().validateRecord(data, schema)._1.equals("valid"))
  }

  test("testProcess") {
    //assert(true)
  }*/

}
