import org.apache.spark.graphx._
import org.apache.spark._
import org.apache.spark.rdd.RDD
import scala.io.Source 
import org.apache.spark.graphx.impl.{EdgePartitionBuilder, GraphImpl}

// val FILENAME = "/Users/vincentpham/Desktop/distributed/training_small.txt"
val FILENAME = "/Users/vincentpham/Desktop/distributed/tinyEWD.txt"
//bin/hadoop fs -get /user/hadoop/data/largeEWD.txt /home/hadoop/
// val FILENAME = "/home/hadoop/training_small.txt"
// val FILENAME = "/home/hadoop/largeEWD.txt"
// val FILENAME = "/Users/vincentpham/Desktop/distributed/largeEWD.txt"

//Set the destination path
val start = 0L
val end = 999999L
val num_nodes = 999999L
val sourceId: VertexId = 0

val start_time = System.currentTimeMillis

var edgeArray = Array(Edge(0L,0L,0F))
// var vertexArray = new Array[(Long,Float)](0)

// var vertexArray = Array((0L,(Float.PositiveInfinity)))
//READ IN THE FILE
// val source = Source.fromFile("/Users/vincentpham/Desktop/distributed/tinyEWD.txt")
val source = Source.fromFile(FILENAME)
val lines = source.getLines.toArray

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
  // vertexArray = vertexArray :+ ((e1,(Float.PositiveInfinity))) 
  // vertexArray = vertexArray :+ ((e2,(Float.PositiveInfinity))) 
}

val vertexArray = sc.parallelize((0L to num_nodes toArray).map{case e => (e, Float.PositiveInfinity)})

val Edge_RDD: RDD[Edge[Float]] = sc.parallelize(edgeArray.slice(1,edgeArray.length))
val vRDD= sc.parallelize(vertexArray)

var graph = Graph(vRDD, Edge_RDD).persist().cache()

val graph_creation_time = System.currentTimeMillis


// var visited = List(start)

//initialize start to
graph = graph.mapVertices((id, attr) => if (id == start) 0 else attr).persist().cache()


val path_distance = graph.pregel(Float.PositiveInfinity)(
  (vertid, d, d_alt) => math.min(d, d_alt), 
  triplet => {  
    if (triplet.srcAttr + triplet.attr < triplet.dstAttr) {
      Iterator((triplet.dstId, triplet.srcAttr + triplet.attr))
    } else {
      Iterator.empty
    }
  },
  (x,y) => math.min(x,y) 
)

val distance = path_distance.vertices.filter{case (id, w) => id == end}.collect()(0)

//display answer
// val distance = unvisited.filter{case (x,y) => x == end}(0)
println("The distance is ", distance)

val algorithm_finish_time = System.currentTimeMillis

val total_time = (algorithm_finish_time - start_time)/1000
val algorithm_time = (algorithm_finish_time - graph_creation_time)/1000
val graph_time = (graph_creation_time - start_time)/1000

println("total time is (in seconds) ", total_time)
println("total algo time is (in seconds) ", algorithm_time)
println("total graph time is (in seconds) ", graph_time)
