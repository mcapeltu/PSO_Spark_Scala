import akka.actor.typed.ActorSystem
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ListBuffer
import scala.concurrent.duration._
import scala.concurrent.{Await, Channel}
import scala.util.Random

class DAPSO(x: List[Array[Double]], y:List[Double], nInputs:Int, nHidden:Int, iters: Int, nParts:Int, pos_max: Double) {


  val conf = new SparkConf()
    .setAppName("PSO Distribuido")
    .setMaster("local[*]")
  val sc = SparkContext.getOrCreate(conf)
  val FA = new Funciones_Auxiliares
  val nWeights: Int = nHidden * (nInputs + 1)
  //Parámetros del PSO
  // Número dimensiones de los pesos
  val n = nWeights
  // Número de partículas
  val m = nParts
  // Número de iteraciones
  val I = iters
  //número de partículas por lote
  val S = 5
  //Otras constantes
  val rand = new Random
  val W = 1.0
  val c_1 = 0.8
  val c_2 = 0.2
  val V_max = 0.6*pos_max
  //Variables del PSO
  var particulas = Array.empty[Array[Double]]
  var mejor_pos_global = Array.empty[Double]
  var best_global_fitness = Double.MaxValue
  // Convertir las listas a arrays serializables
  val xSer: Array[Array[Double]] = x.toArray
  val ySer: Array[Double] = y.toArray
  // Definición de los Channel
  val srch = new Channel[Lote]()
  //val fuch = new Channel[Array[Array[Double]]]()
  val fuch = new Channel[ListBuffer[Array[Double]]]()
  //Inicialización de los vectores "pesos" de la red
  def inicializar_pesos(): Unit = {
    for(i<-0 until m){
      val posicion = Array.fill(nWeights)(FA.Uniform(pos_max, rand))
      val velocidad = Array.fill(nWeights)(FA.Uniform(pos_max, rand))
      val fit = FA.MSERed(xSer, ySer, posicion, nInputs, nHidden)
      val part_ = posicion ++ velocidad ++ posicion ++ Array(fit)
      if (fit < best_global_fitness) {
        best_global_fitness = fit
        mejor_pos_global = posicion
      }
      particulas = particulas :+ part_
    }
  }
  def procesar():Unit ={
    import GlobalEvalActor.{StartSendingMessages, StopSendingMessages, SystemMessage}
    FitnessEvalActor.initialize(srch,fuch,xSer,ySer,nInputs,nHidden,sc)
    GlobalEvalActor.initialize(srch,fuch,nWeights,S,I,m,rand,W,c_1,c_2,V_max, particulas)
    val dapsoController = new DAPSOController(this)
    val globalEvalActor = ActorSystem(GlobalEvalActor(dapsoController),"GlobalEvalActor")

    //val duration = 5.seconds

    globalEvalActor ! StartSendingMessages
    globalEvalActor ! StopSendingMessages
    Await.result(globalEvalActor.whenTerminated, Duration.Inf )
 //   Await.result(globalEvalActor.whenTerminated, duration)
  }

  def get_pesos(): Array[Double] = {
    mejor_pos_global
  }
  def actualizarResultado(resultado: Array[Double], fitness: Double): Unit = {
    mejor_pos_global = resultado
    best_global_fitness = fitness
  }
  def get_best_global_fitness(): Double={
    best_global_fitness
  }
}
