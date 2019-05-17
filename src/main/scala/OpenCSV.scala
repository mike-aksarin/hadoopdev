import java.io.{File, FileReader, StringReader, Writer}

import com.opencsv.{CSVParserBuilder, CSVReader, CSVReaderBuilder, CSVWriter, ICSVParser, ICSVWriter, RFC4180ParserBuilder}

object OpenCSV {

  def separatorChar = ICSVWriter.DEFAULT_SEPARATOR
  def quoteChar = ICSVWriter.NO_QUOTE_CHARACTER
  def escapeChar = '\\'
  def lineEnd = ICSVWriter.DEFAULT_LINE_END

  def eventWriter(buf: Writer): ICSVWriter = {
    new CSVWriter(buf, separatorChar, quoteChar, escapeChar, lineEnd)
  }

  def eventReader(buf: String): CSVReader = {
    //new CSVReader(new StringReader(buf), separatorChar, quoteChar, escapeChar)
    new CSVReaderBuilder(new StringReader(buf))
      .withCSVParser(eventParser())
      .build()
  }

  def eventParser(): ICSVParser = {
    new CSVParserBuilder()
      .withSeparator(separatorChar)
      .withQuoteChar(quoteChar)
      .withEscapeChar(escapeChar)
      .withIgnoreLeadingWhiteSpace(true)
      .build()
  }

  def parseEventLine: String => Array[String] = { line =>
    eventParser().parseLine(Settings.removeEscapedComma(line))
  }

  def countryReader(file: File): CSVReader = {
    new CSVReaderBuilder(new FileReader(file))
      .withCSVParser(countryParser())
      .build()
  }

  def countryParser(): ICSVParser = new RFC4180ParserBuilder().build()

  def parseCountryLine: String => Array[String] = { line =>
    countryParser().parseLine(line)
  }

}
