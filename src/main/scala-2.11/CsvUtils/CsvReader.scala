package CsvUtils

/**
 * @author simonshapiro
 * 
 * This has been configured for github local
 */
import java.io.File

import org.apache.avro.Schema
import org.apache.avro.file.DataFileWriter
import org.apache.avro.generic.{GenericData, GenericDatumWriter}

import scala.collection.mutable.ListBuffer


case class CsvReader(fName:String,firstLineContainsLabels:Boolean,delimiter:Char) {
  
  // TODO Consider writing a csv2arrayset and csv2avro here
  
  def toBuffer:(String,String,ListBuffer[String]) = {
    val returnCode = "success"
    val allLines = new ListBuffer[String]
    val bufferedSource = io.Source.fromFile(fName)
    for (line <- bufferedSource.getLines()) {
      allLines.append(line)
    }
    bufferedSource.close()
// deal with firstLineContainsLabels
    val labels = if(!firstLineContainsLabels) {
        ""
        }
      else allLines.head
    if (firstLineContainsLabels) allLines.remove(0)
    (returnCode,labels,allLines)    
  }
  
  def toAvro(avroSchemaFname:String, avroFname:String): (String,String,String) = {
    val (status,labelLine,allLines) = toBuffer
    println(allLines.length)
    println(labelLine)
    
    val schema = new Schema.Parser().parse(new File(avroSchemaFname))
    println(schema)
    val inFile = new File(fName)
    val lastModified = new java.util.Date(inFile.lastModified)
    println(">>>>",fName,lastModified,inFile.getCanonicalFile)
    /* establish dnaProfile
    val dnaProfile = new GenericData.Record(schema.getField("dnaProfile").schema)  // dnaProfile is special case as it doesn't come from .csv 
    dnaProfile.put("dqPoints", 0)
    dnaProfile.put("underlyingFile", fName)
    dnaProfile.put("lastModified", lastModified.toString)
    */
    val file = new File(avroFname)
    val datumWriter = new GenericDatumWriter[GenericData.Record](schema)
    val dataFileWriter = new DataFileWriter(datumWriter)
    dataFileWriter.create(schema, file)
    
    val labels = labelLine.split(delimiter).map(_.trim)
    println(labels.length)
    allLines.foreach(line=>{
      println(line)
      val user1 = new GenericData.Record(schema)  //strangely this schema only checks for valid fields NOT types.
      val cols = line.split(delimiter).map(_.trim)
      for(i <- cols.indices) {  // interpret schema primitives here
        println(cols.length,labels(i),cols(i))
        if (schema.getField(labels(i)) == null) throw new IllegalArgumentException("csv labels do not match schema file in position "+i+": expecting "+schema.getFields)
        schema.getField(labels(i)).schema.getType match {  //what happens if labels(i) is NOT in schema
          case Schema.Type.INT     => user1.put(labels(i),cols(i).toInt)
          case Schema.Type.LONG     => user1.put(labels(i),cols(i).toLong)
          case Schema.Type.FLOAT     => user1.put(labels(i),cols(i).toFloat)
          case Schema.Type.BOOLEAN => user1.put(labels(i),cols(i).toBoolean)
          case Schema.Type.STRING  => user1.put(labels(i),cols(i).toString)
          case _ => println("SCHEMA CONVERSION ERROR - csv can only contain prmitives")
        }
//        println("out",cols.length,labels(i),cols(i))
      }
//      println(schema.getField("dnaProfile").schema)
//      user1.put("dnaProfile", dnaProfile)  //dnaprofile surpressed
      dataFileWriter.append(user1)
      })
    dataFileWriter.close()
    ("success","",avroFname)
  }

}