import akka.actor.typed.Behavior
import akka.actor.typed.scaladsl.Behaviors

import scala.collection.mutable.ListBuffer
import scala.concurrent._
import scala.util.Random

object GlobalEvalActor {

  import FitnessEvalActor._

  sealed trait SystemMessage

  case object StartSendingMessages extends SystemMessage

  case object ContinueSendingMessages extends SystemMessage

  case object StopSendingMessages extends SystemMessage

  //definimos las variables que necesitamos
  val FA = new Funciones_Auxiliares
  var best_global_fitness = Double.MaxValue
  var mejor_pos_global: Array[Double] = _
  private var srch: Channel[Lote] = _
  private var fuch: Channel[ListBuffer[Array[Double]]] = _
  private var particulas: Array[Array[Double]] = _
  private var N: Int = _
  private var S: Int = _
  private var I: Int = _
  private var m: Int = _
  private var rand: Random = _
  private var W: Double = _
  private var c_1: Double = _
  private var c_2: Double = _
  private var V_max: Double = _
  private var pos_max: Double = _

  //Inicialización
  def initialize(srch: Channel[Lote], fuch: Channel[ListBuffer[Array[Double]]], N: Int, S: Int, I: Int, m: Int, rand: Random, W: Double, c1: Double, c2: Double, pos_max: Double, particulas: Array[Array[Double]]): Unit = {
    GlobalEvalActor.fuch = fuch
    GlobalEvalActor.srch = srch
    GlobalEvalActor.N = N
    GlobalEvalActor.S = S
    GlobalEvalActor.I = I
    GlobalEvalActor.m = m
    GlobalEvalActor.rand = rand
    GlobalEvalActor.W = W
    GlobalEvalActor.c_1 = c1
    GlobalEvalActor.c_2 = c2
    GlobalEvalActor.pos_max = pos_max
    GlobalEvalActor.V_max = 0.6*pos_max
    GlobalEvalActor.particulas = particulas
    ////
    GlobalEvalActor.best_global_fitness = Double.MaxValue
    GlobalEvalActor.mejor_pos_global = Array.empty[Double]
    ////
  }

  def apply(dapsoController: DAPSOController): Behavior[SystemMessage] = Behaviors.setup[SystemMessage] {
    actorContext =>
      val fitnessEvalActor = actorContext.spawn(FitnessEvalActor(), "FitnessEvalActor")
      val lote = new Lote(S)
      //Llenamos la cola 'srch' inicialmente
      for (i <- 0 until m) {
        if (lote.estaCompleto) {
          srch.write(lote.copiar())
          fitnessEvalActor ! ContinueReceivingMessage(actorContext.self)
          lote.clean()
        }
        lote.agregar(particulas(i))
      }
      Behaviors.receiveMessage {
        case StartSendingMessages =>
          val iters = I * m / S
          for (i <- 0 until iters) {
            var sr = fuch.read
            //posicion y velocidad de la partícula
            var pos: Array[Double] = new Array[Double](0)
            var velocidad: Array[Double] = new Array[Double](0)
            //mejor posición local de la partícula
            var mpl: Array[Double] = new Array[Double](0)
            //fitness de la partícula
            var fit: Double = 0
            for (par <- sr) { //recorremos las partículas del channel fuch
              pos = par.slice(0, N) //en la primara rodaja se encuentra la posicion de la particula
              velocidad = par.slice(N, 2 * N) //en la segunda roja se encuentra la velocidad de la particula
              mpl = par.slice(2 * N, 3 * N) //en la tercera rodaja se encuentra la mejor posicion de la particula hasta ese momento
              fit = par(3 * N) //en la última posición está el fitness de la partícula
              if (fit < best_global_fitness) {
                best_global_fitness = fit
                mejor_pos_global = pos
              }
              //actualización: modificación de la partícula con la posición y velocidad actualizadas
              val newPar = FA.posEval(par, mejor_pos_global, N, rand, W, c_1, c_2, V_max, pos_max )

              if (lote.estaCompleto) {
                srch.write(lote.copiar())
                fitnessEvalActor ! ContinueReceivingMessage(actorContext.self)
                lote.clean()
              }
              lote.agregar(newPar)
            }
            sr = null
            pos = null
            velocidad = null
            mpl = null
          }
          fitnessEvalActor ! ContinueReceivingMessage(actorContext.self)
          actorContext.self ! StopSendingMessages
          Behaviors.same
        case StopSendingMessages =>
          dapsoController.recibirResultado(mejor_pos_global, best_global_fitness)
          Behaviors.stopped
      }
  }
}
