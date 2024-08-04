import akka.actor.typed.ActorSystem
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ListBuffer
import scala.concurrent.duration._
import scala.concurrent.{Await, Channel}
import scala.util.Random

class DAPSO(x: List[Array[Double]], y:List[Double], nInputs:Int, nHidden:Int, iters: Int, nParts:Int, pos_max: Double, convergenceCurve_ : Array[Double]) {


  val conf = new SparkConf()
    .setAppName("Distributed Asynchronous PSO --DAPSO")
    .setMaster("local[*]")
    //.setMaster("spark://localhost:7077")
    // .set("spark.executor.resource.gpu.amount", "1")
    // .set("spark.executor.memory", "20G")
    // .setJars(Array("/path-to-the-jar-file/red_neuronal_DSPSO-assembly-0.1.0-SNAPSHOT.jar"))
    //.set("spark.eventLog.dir", "/opt/spark-3.5.0-bin-hadoop3/logs")
    //.set("spark.eventLog.enabled", "true")
    //.set("spark.executor.resource.gpu.discoveryScript", "/opt/sparkRapidsPlugin/getGpusResources.sh")
    //SparkRapids only works with Ubuntu 20.04
    .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    .set("spark.sql.hive.convertMetastoreParquet", "false")
    .set("spark.rdd.compress", "true")
  val sc = SparkContext.getOrCreate(conf)
  val FA = new Auxiliary_Functions
  val nWeights: Int = nHidden * (nInputs + 1)
  // PSO parameters
  // Number of weights dimensions
  val n = nWeights
  // Number of particles
  val m = nParts
  // Number of iterations
  val I = iters
  // Particle number per batch
  val S = 5
  // Other constants
  val rand = new Random
  val W = 1.0
  val c_1 = 0.8
  val c_2 = 0.2
  val V_max = 0.6*pos_max
  
  // PSO variables
  var particles = Array.empty[Array[Double]]
  var best_global_position = Array.empty[Double]
  var best_global_fitness = Double.MaxValue
  // Convert the lists into serializable arrays
  val xSer: Array[Array[Double]] = x.toArray
  val ySer: Array[Double] = y.toArray
  // Channels definition
  val srch = new Channel[Batch]()
  //val fuch = new Channel[Array[Array[Double]]]()
  val fuch = new Channel[ListBuffer[Array[Double]]]()
  // Initializing the vectors
  def init_weights(): Unit = {
    for(i<-0 until m){
      val position = Array.fill(nWeights)(FA.Uniform(pos_max, rand))
      val velocity = Array.fill(nWeights)(FA.Uniform(pos_max, rand))
      val fit = FA.MSERed(xSer, ySer, position, nInputs, nHidden)
      val part_ = position ++ velocity ++ position ++ Array(fit)
      if (fit < best_global_fitness) {
        best_global_fitness = fit
        best_global_position = position
      }
      particles = particles :+ part_
    }
  }
  def processing():Unit ={
    import GlobalEvalActor.{StartSendingMessages, StopSendingMessages, SystemMessage}
    FitnessEvalActor.initialize(srch,fuch,xSer,ySer,nInputs,nHidden,sc)
    GlobalEvalActor.initialize(srch,fuch,nWeights,S,I,m,rand,W,c_1,c_2,V_max, particles, convergenceCurve_)
    val dapsoController = new DAPSOController(this)
    val globalEvalActor = ActorSystem(GlobalEvalActor(dapsoController),"GlobalEvalActor")

    globalEvalActor ! StartSendingMessages
    globalEvalActor ! StopSendingMessages
    Await.result(globalEvalActor.whenTerminated, Duration.Inf )
   }

  def get_weights(): Array[Double] = {
    best_global_position
  }
  def updateResult(resultado: Array[Double], fitness: Double): Unit = {
    best_global_position = resultado
    best_global_fitness = fitness
  }
  def get_best_global_fitness(): Double={
    best_global_fitness
  }
}
