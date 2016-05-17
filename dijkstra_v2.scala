import org.apache.spark.graphx._
import org.apache.spark._
import org.apache.spark.rdd.RDD
import scala.io.Source 
import org.apache.spark.graphx.impl.{EdgePartitionBuilder, GraphImpl}

// val FILENAME = "/Users/vincentpham/Desktop/distributed/training_small.txt"
// val FILENAME = "/Users/vincentpham/Desktop/distributed/tinyEWD.txt"
// hadoop fs -get /user/hadoop/largeEWD.txt /home/hadoop/
// val FILENAME = "/home/hadoop/training_small.txt"
// val FILENAME = "/home/hadoop/largeEWD.txt"
val FILENAME = "/user/hadoop/training_small.txt"
// val FILENAME = "/Users/vincentpham/Desktop/distributed/largeEWD.txt"
// hadoop fs -cp s3://aws-logs-784570650370-us-west-2/elasticmapreduce/data/training_small.txt /user/hadoop/
//Set the destination path
val start = 0L
val end = 9999L
val num_nodes = 9999L

val start_time = System.currentTimeMillis

val rdd = sc.textFile(FILENAME)
val Edge_RDD: RDD[Edge[Float]] = rdd.map{case line => line.trim.split("\\s+")}.filter{case line => line.size > 2}.map{case arr => Edge(arr(0).toLong, arr(1).toLong, arr(2).toFloat)}
val vRDD = sc.parallelize((0L to num_nodes toArray).map{case e => (e, Float.PositiveInfinity)})

var graph = Graph(vRDD, Edge_RDD).persist().cache()
val graph_creation_time = System.currentTimeMillis

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
println("The distance is ", distance)

val algorithm_finish_time = System.currentTimeMillis

val total_time = (algorithm_finish_time - start_time)/1000
val algorithm_time = (algorithm_finish_time - graph_creation_time)/1000
val graph_time = (graph_creation_time - start_time)/1000

println("total time is (in seconds) ", total_time)
println("total algo time is (in seconds) ", algorithm_time)
println("total graph time is (in seconds) ", graph_time)
