import org.apache.spark.graphx._
import org.apache.spark._
import org.apache.spark.rdd.RDD
import scala.io.Source 
import org.apache.spark.graphx.impl.{EdgePartitionBuilder, GraphImpl}

val start_time = System.currentTimeMillis

var edgeArray = Array(Edge(0L,0L,.344324324F))
var vertexArray = new Array[(Long,Float)](0)

// var vertexArray = Array((0L,(Float.PositiveInfinity)))
//READ IN THE FILE
val source = Source.fromFile("/Users/vincentpham/Desktop/distributed/tinyEWD.txt")

val lines = source.getLines.toArray

// //check size
// lines.size

// //check index (with graceful failure)
// lines.lift(10)

//Loop through lines adding to Arrays
for (i <- 2 until lines.length) {
  var triple = lines(i).trim.split("\\s+")
  var e1 = triple(0).toLong
  var e2 = triple(1).toLong
  var e3 = triple(2).toFloat
  if (e1 != e2) {
    val new_edge = Edge(triple(0).toLong, triple(1).toLong, triple(2).toFloat)
    edgeArray = edgeArray :+ new_edge
  }
  vertexArray = vertexArray :+ ((e1,(Float.PositiveInfinity))) 
  vertexArray = vertexArray :+ ((e2,(Float.PositiveInfinity))) 
}

vertexArray = vertexArray.distinct

val Edge_RDD: RDD[Edge[Float]] = sc.parallelize(edgeArray.slice(1,edgeArray.length))
val vRDD= sc.parallelize(vertexArray)

var graph = Graph(vRDD, Edge_RDD)

val graph_creation_time = System.currentTimeMillis


//Set the destination path
val start = 4L
val end = 6L

var visited = List(start)


//initialize start to
graph = graph.mapVertices((id, attr) => if (id == start) 0 else attr)

var unvisited = graph.edges.filter{case e => (e.srcId == start && !visited.contains(e.dstId))}.map(e => (e.dstId, e.attr)).collect()

var reached_end = false
while (unvisited.size > 0 && !reached_end) {

  unvisited = unvisited.sortBy(_._2)

  //Take the values of the lowest distance
  val node = unvisited(0)
  val vertexid = node._1
  val weight = node._2

  //Take neighbors of the lowest distance
  var neighbors = graph.edges.filter{case e => (e.srcId == vertexid && !visited.contains(e.dstId))}.map(e => (e.dstId, e.attr)).collect()

  for (i <- 0 until neighbors.length){
    val curr_node = neighbors(i)
    val curr_id = curr_node._1
    val curr_weight = curr_node._2 + weight

    if (curr_id == end) {
      reached_end = true
    }
    if (!unvisited.map{case (x,w) => x}.contains(curr_id)) {
      unvisited  = unvisited :+ (curr_id, curr_weight)
    } else {
      unvisited = unvisited.map{case (x, w) =>  if (x == curr_id && curr_weight < w) (x, curr_weight) else (x, w)}
    }
  }

  visited = visited :+ vertexid
  unvisited = unvisited.slice(1,unvisited.length) //removes visited node
}//then goes back up to sorting

//display answer
val distance = unvisited.filter{case (x,y) => x == end}(0)
println("The distance is ", distance)

val algorithm_finish_time = System.currentTimeMillis

val total_time = (algorithm_finish_time - start_time)/1000
val algorithm_time = (algorithm_finish_time - graph_creation_time)/1000
val graph_time = (graph_creation_time - start_time)/1000

println("total time is (in seconds) ", total_time)
println("total algo time is (in seconds) ", algorithm_time)
println("total graph time is (in seconds) ", graph_time)
