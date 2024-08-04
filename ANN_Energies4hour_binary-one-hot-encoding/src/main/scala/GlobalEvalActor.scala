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
  val FA = new Auxiliary_Functions
  var best_global_fitness = Double.MaxValue
  var best_global_pos: Array[Double] = _
  private var srch: Channel[Batch] = _
  private var fuch: Channel[ListBuffer[Array[Double]]] = _
  private var particles: Array[Array[Double]] = _
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
  private var convergenceCurve: Array[Double]=_
  //Inicialization
  def initialize(srch: Channel[Batch], fuch: Channel[ListBuffer[Array[Double]]], N: Int, S: Int, I: Int, m: Int, rand: Random, W: Double, c1: Double, c2: Double, pos_max: Double, particulas: Array[Array[Double]], convergenceCurve_ : Array[Double]): Unit = {
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
    GlobalEvalActor.particles = particulas
    ////
    GlobalEvalActor.best_global_fitness = Double.MaxValue
    GlobalEvalActor.best_global_pos = Array.empty[Double]
    GlobalEvalActor.convergenceCurve= convergenceCurve_
    ////
  }

  def apply(dapsoController: DAPSOController): Behavior[SystemMessage] = Behaviors.setup[SystemMessage] {
    actorContext =>
      val fitnessEvalActor = actorContext.spawn(FitnessEvalActor(), "FitnessEvalActor")
      val batch = new Batch(S)
      //We fill up the queue 'srch' initially
      for (i <- 0 until m) {
        if (batch.isFull) {
          srch.write(batch.copy())
          fitnessEvalActor ! ContinueReceivingMessage(actorContext.self)
          batch.clean()
        }
        batch.aggregate(particles(i))
      }
      Behaviors.receiveMessage {
        case StartSendingMessages =>
          val iters = I * m / S
          for (i <- 0 until iters) {
            var sr = fuch.read
            // particle's position and velocity
            var pos: Array[Double] = new Array[Double](0)
            var velocidad: Array[Double] = new Array[Double](0)
            //best local position of the particle
            var mpl: Array[Double] = new Array[Double](0)
            //particle's fitness
            var fit: Double = 0
            for (par <- sr) { //we go through the particles of channel fuch
              pos = par.slice(0, N) // in the first slice you will find the position of the particle
              velocidad = par.slice(N, 2 * N) // on the second slice is the particle velocity
              mpl = par.slice(2 * N, 3 * N) // in the third slice is the best position of the particle so far
              fit = par(3 * N) // in the last position is the particle fitness
              if (fit < best_global_fitness) {
                best_global_fitness = fit
                best_global_pos = pos
              }
              // update: modification of the particle with updated position and velocity
              val newPar = FA.posEval(par, best_global_pos, N, rand, W, c_1, c_2, V_max, pos_max )

              if (batch.isFull) {
                srch.write(batch.copy())
                fitnessEvalActor ! ContinueReceivingMessage(actorContext.self)
                batch.clean()
              }
              batch.aggregate(newPar)
            }
            convergenceCurve(i)= best_global_fitness
            sr = null
            pos = null
            velocidad = null
            mpl = null
          }
          fitnessEvalActor ! ContinueReceivingMessage(actorContext.self)
          actorContext.self ! StopSendingMessages
          Behaviors.same
        case StopSendingMessages =>
          dapsoController.receiveResult(best_global_pos, best_global_fitness)
          Behaviors.stopped
      }
  }
}
