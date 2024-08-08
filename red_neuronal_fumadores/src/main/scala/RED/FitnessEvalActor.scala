package RED

import scala.concurrent._
import akka.actor.{Actor, ActorSystem, Props}
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import scala.collection.mutable.ListBuffer

import akka.actor.{Actor, ActorRef, Props}
import scala.concurrent.duration._

import akka.actor.typed.scaladsl.{ActorContext, Behaviors}
import akka.actor.typed.{ActorRef, Behavior, ActorSystem}


object FitnessEvalActor {
  import RED.GlobalEvalActor.SystemMessage
  sealed trait MessageToFitnessActor
  final case class ContinueReceivingMessage() extends MessageToFitnessActor
  // Variables definition
  val FA = new Auxiliary_Functions
  private var srch: Channel[Batch] = _
  private var fuch: Channel[ListBuffer[Array[Double]]] = _
  private var x: Array[Array[Double]] = _
  private var y: Array[Double] = _
  private var nInputs: Int = _
  private var nHidden: Int = _
  private var sc: SparkContext = _
  private var tresh: Double = _

  // Initilization method
  def initialize(
                  srch: Channel[Batch], fuch: Channel[ListBuffer[Array[Double]]], data: Array[Array[Double]],
                  y: Array[Double], nInputs: Int, nHidden: Int, sc: SparkContext
                ): Unit = {
    FitnessEvalActor.srch = srch
    FitnessEvalActor.fuch = fuch
    FitnessEvalActor.x = data
    FitnessEvalActor.y = y
    FitnessEvalActor.nInputs = nInputs
    FitnessEvalActor.nHidden = nHidden
    FitnessEvalActor.sc = sc
  }

  private def processing(): Unit = {
    //println("Evaluating fitness")
    val batch = srch.read
    //val psfu = batch.obtainBatch.map(x => FE.fitnessEval(x, n, objetivo))
    val aux = batch.obtainBatch.toArray
    val RDD = sc.parallelize(aux)
    val psfu_array = RDD.map(part => FA.fitnessEval(x, y, part, nInputs, nHidden)).collect()
    val psfu = FA.toListBuffer(psfu_array)
    fuch.write(psfu)
  }

  def apply(): Behavior[MessageToFitnessActor] = Behaviors.setup {
    context: ActorContext[MessageToFitnessActor] =>
      Behaviors.receiveMessage { message =>
        message match {
          //case ContinueReceivingMessage(sender) =>
          case ContinueReceivingMessage() =>
            processing()
            Behaviors.same
        }
      }
  }
}
