import scala.io.Source
import scala.language.postfixOps

class CSVReader(fileName: String) {
  def readCSV(): List[List[String]] = {
    val bufferedSource = Source.fromFile(fileName)
    val columns = bufferedSource.getLines.map { line =>
      val cols = line.split(",").map(_.trim)
      cols.toList
    }.toList
    bufferedSource.close()
    columns
  }

}
