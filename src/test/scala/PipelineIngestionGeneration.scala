import com.lbp.ingestion.Main

import org.scalatest.FunSuite
import org.scalatest.{ConfigMap }


class PipelineIngestionGeneration extends FunSuite   {
  val config = ConfigMap.apply(("optionName", "--app-name"),
                              ("valueName", "zob"),
                              ("optionPattern", "--pattern"),
                              ("valuePattern", "(?<filename>qtg0[0-9]{2}aa.aflaall).*"),
                              ("optionMode", "--mode"),
                              ("valueMode", "APPEND"))

  test("IngestionSourceRaw ValidRecord ") {
    def IngestionSourceRaw(configMap: ConfigMap) = {
      val check = GenerateRecords.generateValidRecords(1000)
      assert(check == true)
      Main.main(
          Array(configMap.get("optionName").get.toString,
          configMap.get("valueName").get.toString,
          configMap.get("optionPattern").get.toString,
          configMap.get("valuePattern").get.toString,
          configMap.get("optionMode").get.toString,
          configMap.get("valueMode").get.toString)
      )
    }
    IngestionSourceRaw(config)
  }

  test("IngestionSourceRaw InvalidRecord ") {
    def IngestionSourceRaw(configMap: ConfigMap) = {
      GenerateRecords.generateInvalidRecords(10, 20.0)
      Main.main(
        Array(configMap.get("optionName").get.toString,
          configMap.get("valueName").get.toString,
          configMap.get("optionPattern").get.toString,
          configMap.get("valuePattern").get.toString,
          configMap.get("optionMode").get.toString,
          configMap.get("valueMode").get.toString)
      )
    }
    IngestionSourceRaw(config)
  }
}
