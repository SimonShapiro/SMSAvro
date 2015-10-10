

/**
 * @author simonshapiro
 */
import java.io.File

import CsvReader.CsvReader
import org.apache.avro.Schema
import org.apache.avro.file.DataFileReader
import org.apache.avro.generic.{GenericData, GenericDatumReader}

import scala.collection.mutable.HashMap

//    val (status,labels,allLines) = CsvReader(args(0),true,",").csv2Buffer
//    val g=Graph(allLines)
//    println(g.asXML)


object csv2avro {

  def processArgs(args:Array[String]):HashMap[String,String] = {
    val argHash = new HashMap[String,String]
    argHash.put("input","data/input.csv")
    argHash.put("output","data/output.avro")
    argHash.put("schema","data/csv.schema")
    args.foreach(l=>{
      val ll = l.split("=")  // .map(_.trim) is unnecessary because args will misuse white space around '='
      argHash.put(ll(0),ll(1))
    })
    println(argHash)
    argHash
  }

  def main(args:Array[String]){
    val argHash = processArgs(args)
    val (status,msg,avroFname) = new CsvReader(argHash("input"),true,',').toAvro(argHash("schema"),argHash("output"))
  
    // Deserialize users from disk
    val file = new File(argHash("output"))
    val schemaParser = new Schema.Parser()
    val schema = schemaParser.parse(new File(argHash("schema")))
    println(schema)
    val datumReader = new GenericDatumReader[GenericData.Record](schema)
    println(datumReader.getSchema)
    val dataFileReader = new DataFileReader[GenericData.Record](file, datumReader)
    var user:GenericData.Record = null
    while (dataFileReader.hasNext) {
      // Reuse user object by passing it to next(). This saves us from
      // allocating and garbage collecting many objects for files with
      // many items.
        user = dataFileReader.next(user)
//      println(user);
//      println(user.get("first_name"))
    }
    println(dataFileReader.getSchema)
  }
}