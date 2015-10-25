import CsvUtils.CsvReader

/**
 * Created by simonshapiro on 24/10/15.
 */
object csv2avro {
  def main(args: Array[String]) {
    if (args.length != 5) {
      println("Usage: csv2avroCL <path to/csv file> <header in firstline (true/false)> <path to/schema file> <path to/avro file> <separator>")
    }
    else {
      val csvFileName = args(0)
      val headerInFirstLine = args(1) match {
        case "true" => true
        case _ => false
      }
      val schemaFileName = args(2)
      val avroFileName = args(3)
      val separator = args(4).toCharArray
      val (status,msg,avroFname) = new CsvReader(csvFileName,headerInFirstLine,separator(0))
        .toAvro(schemaFileName,avroFileName)
      println(status)
    }
  }
}
