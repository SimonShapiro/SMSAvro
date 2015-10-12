package CsvUtils

/**
 * Created by simonshapiro on 10/10/15.
 */

import java.io.File

import org.apache.avro.Schema
import org.apache.avro.file.{DataFileReader, DataFileWriter}
import org.apache.avro.generic.{GenericDatumReader, GenericData, GenericDatumWriter}

import scala.collection.mutable
import scala.collection.mutable.ListBuffer


case class AvroWriter(avroFname:String,avroSchemaFname:String) {

  def flattenIntoBuffer(separator:String):ListBuffer[String] = {

    def unpackRecord(rec: GenericData.Record, schema: org.apache.avro.Schema, collector:mutable.ListBuffer[String]): mutable.ListBuffer[String] = {
      var field: Schema.Field = null
      val fieldIterator = schema.getFields.iterator
      while (fieldIterator.hasNext) {
        field = fieldIterator.next()
        field.schema.getType match {
          case Schema.Type.STRING => collector += '"'+rec.get(field.name).toString+'"'
          case Schema.Type.RECORD =>  {
            val child = rec.get(field.name).asInstanceOf[GenericData.Record]
            unpackRecord(child, schema.getField(field.name).schema,collector) //unpackrecord(???)
          }
          case _ => collector += rec.get(field.name).toString
        }
        println(field.name,field.schema.getType, rec.get(field.name))
        if (field.schema.getType == Schema.Type.RECORD) {
        }
      }
      println(collector.size)
      collector
    }

    def unpackNamesFromSchema(parentName: String, schema: org.apache.avro.Schema, collector:mutable.MutableList[String]):mutable.MutableList[String] = {
      var field: Schema.Field = null
      val fieldIterator = schema.getFields.iterator
      while (fieldIterator.hasNext) {
        field = fieldIterator.next()
        collector += '"'+parentName+"."+field.name+'"'
        if (field.schema.getType == Schema.Type.RECORD) {
          unpackNamesFromSchema(parentName+"."+field.name, schema.getField(field.name).schema, collector)
        }
      }
      collector
    }

    val outBuffer = new ListBuffer[String]
    val file = new File(avroFname)
    val schemaParser = new Schema.Parser()
    val schema = schemaParser.parse(new File(avroSchemaFname))
    println(schema)
    val datumReader = new GenericDatumReader[GenericData.Record](schema)
    val dataFileReader = new DataFileReader[GenericData.Record](file, datumReader)
    var user:GenericData.Record = null
//    var field:Schema.Field = null
    var collector = new mutable.MutableList[String]
    val parentName = file.getName.split('.')(0)
    collector = unpackNamesFromSchema(parentName,schema,collector)
    outBuffer += collector.mkString(separator)
    println(collector.mkString(separator))

    while (dataFileReader.hasNext) {
      // Reuse user object by passing it to next(). This saves us from
      // allocating and garbage collecting many objects for files with
      // many items.
      user = dataFileReader.next(user)
      val collector = new mutable.ListBuffer[String]
      val out = unpackRecord(user, schema,collector)
      outBuffer += out.mkString(separator)
      println(out.mkString(separator))
/*
//      val fieldIterator = user.getSchema.getFields.iterator   //unpackrecord(???)
      while(fieldIterator.hasNext) {
        field = fieldIterator.next()
        println(field)
        println(field.schema.getType,user.get(field.name))
        if (field.schema.getType == Schema.Type.RECORD) {   //unpackrecord(???)
          val child = user.get(field.name).asInstanceOf[GenericData.Record]
          println("got record with schema ",schema.getField(field.name).schema.getFields,child.get("dqPoints"))
          val childDatumReader = new GenericDatumReader[GenericData.Record](field.schema)
        }
      }
  //    println("fields",schema.getFields)
       println(user.get("first_name"))
    */
    }
    outBuffer
  }
}
