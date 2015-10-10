package YedIntegration

/**
 * @author simonshapiro
 */

import scala.collection.mutable.ListBuffer
import scala.collection.mutable.HashMap

case class Graph(allLines:ListBuffer[String]) {

  val nodeList = new HashMap[String,Node]
  val edgeList = new HashMap[String,Edge]
  for (line <- allLines) {
    val cols = line.split(",").map(_.trim)
    // do whatever you want with the columns here
    edgeList.put(cols(2),Edge(cols(0),cols(1),cols(2),cols(3),cols(4)))
    if (!nodeList.contains(cols(0))) nodeList(cols(0)) = Node(cols(0),cols(0),"rectangle")
    if (!nodeList.contains(cols(1))) nodeList(cols(1)) = Node(cols(1),cols(1),"rectangle")
  }
  
  val asXML =   
<graphml xmlns="http://graphml.graphdrawing.org/xmlns"   
         xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"  
         xsi:schemaLocation="http://graphml.graphdrawing.org/xmlns 
           http://www.yworks.com/xml/schema/graphml/1.1/ygraphml.xsd"  
         xmlns:y="http://www.yworks.com/xml/graphml">  
  <key id="d0" for="node" yfiles.type="nodegraphics"/>  
  <key id="d1" for="edge" yfiles.type="edgegraphics"/>  
  <graph id="G" edgedefault="directed">
  {nodeList.map {n => {n._2.toXML}}}
  {edgeList.map {e => {e._2.toXML}}}
</graph>
</graphml>  
}
/*
<?xml version="1.0" encoding="UTF-8"?>  
<!-- This file was written by the JAVA GraphML Library. -->  
 * 
  nodeList.foreach(n=>n._2.toXML)
 */
case class Node(id:String,name:String,shape:String) {
  val yShapes = List("rectangle",
                      "elipse",
                      "heaxagon",
                      "octogon")  //see http://www.yworks.com/xml/schema/graphml/1.0/doc/http___www.yworks.com_xml_graphml/simpleType/shapeType.type.html
  val nodeShape = if (yShapes.contains(shape)) {
                    shape
                  } 
                  else {
                    "roundrectangle"  //default
                  }
  def toXML = {
    <node id={id}>  
      <data key="d0">  
        <y:ShapeNode selected="true">  
          <y:Geometry x="170.5" y="-15.0" width="59.0" height="30.0"/>  
          <y:Fill color="#CCCCFF" transparent="false"/>  
          <y:BorderStyle type="line" width="1.0" color="#000000"/>  
          <y:NodeLabel>{name}</y:NodeLabel>  
          <y:Shape type={nodeShape}/>  
        </y:ShapeNode>  
      </data>  
    </node>  
  }
}

case class Edge(source:String,target:String,id:String,label:String,purpose:String) {
  val purposeDisplay = purpose match {
    case "xx" => "#CCCCFF"
    case _    => "#000000"
  }
  def toXML = {
    <edge id={id} source={source} target={target}>  
      <data key="d1">  
        <y:PolyLineEdge>  
          <y:LineStyle type="line" width="1.0" color={purposeDisplay}/>  
          <y:Arrows source="none" target="standard"/>  
          <y:EdgeLabel>{label}</y:EdgeLabel>  
          <y:BendStyle smoothed="false"/>  
        </y:PolyLineEdge>  
      </data>  
    </edge>  
   }
}

/*
 * 
 * 
 * GRAPHML graph template
 * 
 * 
 * <?xml version="1.0" encoding="UTF-8"?>  
<!-- This file was written by the JAVA GraphML Library. -->  
<graphml xmlns="http://graphml.graphdrawing.org/xmlns"   
         xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"  
         xsi:schemaLocation="http://graphml.graphdrawing.org/xmlns 
           http://www.yworks.com/xml/schema/graphml/1.1/ygraphml.xsd"  
         xmlns:y="http://www.yworks.com/xml/graphml">  
  <key id="d0" for="node" yfiles.type="nodegraphics"/>  
  <key id="d1" for="edge" yfiles.type="edgegraphics"/>  
  <graph id="G" edgedefault="directed">  
 * 
 * 
     <edge id="e1" source="n1" target="n0">  
      <data key="d1">  
        <y:PolyLineEdge>  
          <y:Path sx="0.0" sy="-15.0" tx="29.5" ty="0.0">  
            <y:Point x="425.0" y="0.0"/>  
          </y:Path>  
          <y:LineStyle type="dashed" width="5.0" color="#CCCCFF"/>  
          <y:Arrows source="white_diamond" target="standard"/>  
          <y:EdgeLabel>Happy New Year!</y:EdgeLabel>  
          <y:BendStyle smoothed="false"/>  
        </y:PolyLineEdge>  
      </data>  
    </edge>  
 */
