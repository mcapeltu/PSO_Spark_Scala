package RED

import akka.actor.typed.ActorSystem
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ListBuffer
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Channel}
import scala.util.Random

class DAPSO(x: List[Array[Double]], y:List[Double], nInputs:Int, nHidden:Int, iters: Int, nParts:Int, pos_max: Double, c1: Double, c2: Double) {
  /*val conf = new SparkConf()
    .setAppName("PSO Distribuido")
    .setMaster("local[*]")
  val sc = SparkContext.getOrCreate(conf)*/

  val conf = new SparkConf()
    .setAppName("DAPSO")
    .setMaster("local[*]")
    /*.set("spark.rapids.sql.concurrentGpuTasks", "1")
    .set("spark.executor.resource.gpu.amount", "1")
    .set("spark.executor.resource.gpu.discoveryScript", "/opt/sparkRapidsPlugin/getGpusResources.sh")
    .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    .set("spark.plugins", "com.nvidia.spark.SQLPlugin")*/
  val sc = SparkContext.getOrCreate(conf)

  val FA = new Auxiliary_Functions
  val nWeights: Int = nHidden * (nInputs + 1)
  //PSO parameters
  // Weigths dimension
  val n = nWeights
  // NUmber of particles
  val m = nParts
  // NUmber of iterations
  val I = iters
  //NUmber of particles per batch
  val S = 5
  //Other constants
  val rand = new Random
  val W = 1.0
  val c_1 = c1
  val c_2 = c2
  val V_max = 0.1*pos_max
  //PSO variables
  var particles = Array.empty[Array[Double]]
  var best_global_pos = Array.empty[Double]
  var best_global_fitness = Double.MaxValue
  // Convert lists into serializable arrays
  val xSer: Array[Array[Double]] = x.toArray
  val ySer: Array[Double] = y.toArray
  // Channels definition
  val srch = new Channel[Batch]()
  //val fuch = new Channel[Array[Array[Double]]]()
  val fuch = new Channel[ListBuffer[Array[Double]]]()
  //Initialization of the net weight vectors
  def initialize_weights(): Unit = {
    for(i<-0 until m){
      val position = Array.fill(nWeights)(FA.Uniform(pos_max, rand))
      val velocity = Array.fill(nWeights)(FA.Uniform(pos_max, rand))
      val fit = FA.binaryEntropyRed(xSer, ySer, position, nInputs, nHidden)
      val part_ = position ++ velocity ++ position ++ Array(fit)
      if (fit < best_global_fitness) {
        best_global_fitness = fit
        best_global_pos = position
      }
      particles = particles :+ part_
    }
  }
  def processing():Unit ={
    import GlobalEvalActor.{StartSendingMessages, StopSendingMessages, SystemMessage}
    FitnessEvalActor.initialize(srch,fuch,xSer,ySer,nInputs,nHidden,sc)
    GlobalEvalActor.initialize(srch,fuch,nWeights,S,I,m,rand,W,c_1,c_2,pos_max, particles)
    val dapsoController = new DAPSOController(this)
    val globalEvalActor = ActorSystem(GlobalEvalActor(dapsoController),"GlobalEvalActor")

    globalEvalActor ! StartSendingMessages
    globalEvalActor ! StopSendingMessages
    Await.result(globalEvalActor.whenTerminated, Duration.Inf )
  }
  def get_weights(): Array[Double] = {
    best_global_pos
  }
  def updateResult(result: Array[Double], fitness: Double): Unit = {
    best_global_pos = result
    best_global_fitness = fitness
  }
  def get_best_global_fitness(): Double={
    best_global_fitness
  }
}