package RED
import scala.concurrent._

import scala.util.Random
import RED.Auxiliary_Functions

import scala.collection.mutable.ListBuffer

import akka.actor.typed.scaladsl.{ActorContext, Behaviors}
import akka.actor.typed.{ActorRef, Behavior, ActorSystem}

// Define an actor to assess the global best and update asynchronously
object GlobalEvalActor {
  import FitnessEvalActor._
  sealed trait SystemMessage
  case object StartSendingMessages extends SystemMessage
  case object ContinueSendingMessages extends SystemMessage
  case object StopSendingMessages extends SystemMessage
  //We define the needed variables
  val FA = new Auxiliary_Functions
  var best_global_fitness = Double.MaxValue
  var best_global_pos: Array[Double] = _
  private var fuch: Channel[ListBuffer[Array[Double]]] = _
  private var srch: Channel[Batch] = _
  private var N: Int = _
  private var S: Int = _
  private var I: Int = _
  private var m: Int = _
  private var rand: Random = _
  private var W: Double = _
  private var c_1: Double = _
  private var c_2: Double = _
  private var V_max: Double = _
  private var particles: Array[Array[Double]] = _
  private var pos_max: Double = _

  def initialize(srch: Channel[Batch], fuch: Channel[ListBuffer[Array[Double]]], N: Int, S: Int, I: Int, m: Int,
                 rand: Random, W: Double, c1: Double, c2: Double, pos_max: Double, particulas: Array[Array[Double]]): Unit = {
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
    GlobalEvalActor.V_max = 0.1*pos_max
    GlobalEvalActor.pos_max = pos_max
    GlobalEvalActor.particles = particulas
    GlobalEvalActor.best_global_fitness = Double.MaxValue
    GlobalEvalActor.best_global_pos = Array.empty[Double]
  }

  def apply(dapsoController: DAPSOController): Behavior[SystemMessage] = Behaviors.setup[SystemMessage] {
    actorContext =>
      val fitnessEvalActor = actorContext.spawn(FitnessEvalActor(), "FitnessEvalActor")
      //val resultsActor = actorContext.spawn(ResultsActor(), "ResultsActor")
      val batch = new Batch(S)
      //we add the particles to the queue
      for (i <- 0 until m) {
        if (batch.isFull) {
          srch.write(batch.copy())
          fitnessEvalActor ! ContinueReceivingMessage()
          batch.clean()
        }
        batch.aggregate(particles(i))
      }
      Behaviors.receiveMessage {
        case StartSendingMessages =>
          //println("Start Global Actor task")
          //resultsActor ! StartTime()
          val iters = I * m / S
          for (i <- 0 until iters) {
            //println("iter " + i + " dentro de globalActor") for debugging
            // Wait for one element from fuch channel
            var sr = fuch.read
            var pos: Array[Double] = new Array[Double](0)
            var velocity: Array[Double] = new Array[Double](0)
            var mpl: Array[Double] = new Array[Double](0)
            var fit: Double = 0
            for (par <- sr) {
              pos = par.slice(0, N)
              velocity = par.slice(N, 2 * N)
              mpl = par.slice(2 * N, 3 * N)
              fit = par(3 * N)
              if (fit < best_global_fitness) {
                best_global_fitness = fit
                best_global_pos = mpl
              }
              val newPar = FA.posEval(par, best_global_pos, N, rand, W, c_1, c_2, V_max, pos_max)
              if (batch.isFull) {
                srch.write(batch.copy())
                fitnessEvalActor ! ContinueReceivingMessage()
                batch.clean()
              }
              batch.aggregate(newPar)
            }
            sr = null
            //println("best current position " + best_global_pos.mkString(", "))
            pos = null
            velocity = null
            mpl = null
          }
          actorContext.self ! StopSendingMessages
          Behaviors.same
        case StopSendingMessages =>
          //resultsActor ! EndTime()
          //println("The global results of the calculation!!!")
          //println(s"best_global_pos-> ${best_global_pos.mkString("[", ", ", "]")}")
          //println(s"best_global_fitness-> $best_global_fitness")
          dapsoController.receiveResult(best_global_pos, best_global_fitness)
          Behaviors.stopped
      }
  }
}