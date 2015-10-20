

/**
 * @author simonshapiro
 */
import java.io.File
import scala.xml.XML

import CsvUtils.{AvroWriter, CsvReader}
import org.apache.avro.Schema
import org.apache.avro.file.DataFileReader
import org.apache.avro.generic.{GenericData, GenericDatumReader}

import scala.collection.mutable
import scala.collection.mutable.HashMap

//    val (status,labels,allLines) = CsvReader(args(0),true,",").csv2Buffer
//    val g=Graph(allLines)
//    println(g.asXML)


object csv2avro {
  def main(args:Array[String]){
    println("Received {args.length}", args.length)
    val config = XML.loadFile(args(0))

    val (status,msg,avroFname) = new CsvReader((config \ "inputCSV").text
        ,{((config \ "firstRowHasLabels").text match {
            case "true" => true
            case _ => false
    })}
        ,(config \ "primarySeparator").text(0))
      .toAvro((config \ "schemaCSV").text,(config \ "outputAVRO").text)

    val wr = new AvroWriter((config \ "outputAVRO").text,(config \ "selectionSchema").text)
      .writeCsvAfterExpanding((config \ "idField").text,
        (config \ "expansionField").text,
        (config \ "secondarySeparator").text(0),
        (config \ "expandedCSV").text,
        (config \ "primarySeparator").text)

    // Deserialize users from disk
/*
//  Only when read schema differs
    val schemaParser = new Schema.Parser()
    val schema = schemaParser.parse(new File(argHash("schema")))
    println(schema)


    val file = new File(argHash("output"))
    val datumReader = new GenericDatumReader[GenericData.Record](schema)
    println("provided schema",datumReader.getSchema)
    val dataFileReader = new DataFileReader[GenericData.Record](file, datumReader)
    var user:GenericData.Record = null
    while (dataFileReader.hasNext) {
        user = dataFileReader.next(user)
    }
    println(dataFileReader.getSchema)
*/
    println("hello")
  }
}