import java.io.{FileWriter, BufferedWriter, File}
import scala.collection.mutable.ListBuffer

/**
 * Created by simonshapiro on 27/10/15.
 */


object extractDraftSchema {
  def extract(fName: String, separator: String, namespace: String, draftFile: String): Unit = {
    val (status, labels, allLines) = CsvUtils.CsvReader(fName, true, separator(0)).toBuffer
    val listOfLabels = labels.split(separator(0)).map(_.trim)
    val schemaStrings = new ListBuffer[String]
    schemaStrings += "{ \"namespace\": \"%s\",".format(namespace)
    schemaStrings += "  \"name\":  \"%s\",".format(fName.split("/").last)
    schemaStrings += "  \"type\":  \"record\","
    schemaStrings += "  \"fields\": ["
    val numberOfCols = listOfLabels.length
    var count = 0
    listOfLabels.foreach(label => {
      count += 1
      val avroCompatibleLabel = label.replaceAll("[' ']","_").replaceAll("[@|//?%-]","")
      if (count == numberOfCols) schemaStrings += "          {\"name\": \"%s\", \"type\": \"string\"}".format(avroCompatibleLabel)
      else schemaStrings += "          {\"name\": \"%s\", \"type\": \"string\"},".format(avroCompatibleLabel)
    })
    schemaStrings += "     ]"
    schemaStrings += "}"
    println(schemaStrings.mkString("\n"))
    val file = new File(draftFile)
    val bw = new BufferedWriter(new FileWriter(file))
    bw.write(schemaStrings.mkString("\n"))
    bw.close()
  }

  def main(args: Array[String]) {
    if (args.length != 4) {
      println("Usage <path to/csv file> <delimiter> <namespace> <path to/draft schema file>")
    }
    else {
      val fName = args(0)
      val separator = args(1)
      val namespace = args(2)
      val draftFile = args(3)
      extract(fName, separator, namespace, draftFile)
    }
  }
}
