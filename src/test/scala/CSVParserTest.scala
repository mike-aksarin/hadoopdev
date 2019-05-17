import org.scalatest.{FlatSpec, Matchers}

class CSVParserTest  extends FlatSpec with Matchers {

  it should "parse country blocks" in {
    val parsed = OpenCSV.parseCountryLine("147.203.120.0/24,,6252001,,1,0")
    parsed should contain theSameElementsInOrderAs Seq("147.203.120.0/24", "", "6252001", "", "1", "0")
  }

  it should "parse escaped events" in {
    val parsed = OpenCSV.parseEventLine("Methusela's lamp\\, or The last battle of chekists with masons")
    parsed should have size 1
  }

}
