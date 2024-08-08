package RED

import RED.Auxiliary_Functions

import java.io.{File, FileWriter, PrintWriter}
import scala.io.Source
import akka.actor.typed.ActorSystem

object Main {

  val FA = new Auxiliary_Functions

  def main(args: Array[String]): Unit = {
    val fileNameTraining = "training_feature.csv"
    val fileNameTest = "test_feature.csv"
    val numRowsToKeep: Int = 1000 // Number of rows to keep

    var dataRows = Source.fromFile(fileNameTraining).getLines.drop(1).filter { line =>
      val cols = line.split(",").map(_.trim)
      cols.forall(_.nonEmpty) // Filters rows with non empty values
    }.take (numRowsToKeep).toList.map { line =>
      val cols = line.split(",").map(_.trim)
      cols
    }
    val dataListString: List[Array[String]] = dataRows.map(_.take(68))
    //val data: List[Array[Double]] = dataListString.map(_.map(_.toDouble))
    var valueString = dataRows.map(_(68))
    val value: List[Double] = valueString.map {
      case "0" => 0.0
      case "1" => 1.0
      case _ => throw new IllegalArgumentException("Not valid value in list")
    }
    val rows = dataRows.length
    val columns = if (rows > 0) dataRows.head.length else 0

    println(s"The number of rows is: $rows")
    println(s"The number of columns is: $columns")

    //Similar for test dataset
    var dataRowsTest = Source.fromFile(fileNameTest).getLines.drop(1).filter { line =>
      val cols = line.split(",").map(_.trim)
      cols.forall(_.nonEmpty) // Filters rows with non empty values
    }.toList.map { line =>
      val cols = line.split(",").map(_.trim)
      cols
    }
    val dataListStringTest: List[Array[String]] = dataRowsTest.map(_.take(68))
    //val dataTest: List[Array[Double]] = dataListStringTest.map(_.map(_.toDouble))
    var valueStringTest = dataRowsTest.map(_(68))
    val valueTest: List[Double] = valueStringTest.map {
      case "0" => 0.0
      case "1" => 1.0
      case _ => throw new IllegalArgumentException("Not valid value in list")
    }

    val data = dataListString.map(_.map(_.toDouble))
    val dataTest = dataListStringTest.map(_.map(_.toDouble))
    //number of features
    val nInputs: Int = data.headOption.map(_.length).getOrElse(0)
    //number of neurons in the hidden layer
    val nHidden: Int =  (1.9*nInputs).toInt
    val I = 100
    val m = 100
    val pos_max = 1.0
    val c1 = 3.5
    val c2 = 1.8

    val existingFile = new FileWriter(s"DAPSO_results_$I _$m _$pos_max _$c1 _$c2.txt", true)
    val outputFile = new PrintWriter(existingFile)

    //Sequential PSO execution
    val trainer = new sequential_pso(data, value, nInputs, nHidden, I, m, pos_max, c1, c2)
    //DAPSO algorithm run
    //val trainer = new DAPSO(data, value, nInputs, nHidden, I, m, pos_max, c1, c2)
    trainer.initialize_weights()
    val start = System.nanoTime()
    trainer.processing()
    val end = System.nanoTime()
    val weights = trainer.get_weights()
    val duration = (end - start) / 1e9
    var predicted: List[Double] = List()
    for (i <- 0 until dataTest.length) {
      val prDouble = FA.forwardPropClas(dataTest(i), weights, nInputs, nHidden)
      val pr = FA.threshold_sign(prDouble, 0.5)
      predicted = predicted :+ pr
    }
    for ((real, prediction) <- valueTest.zip(predicted)) {
      //println(s"Real: $real - Predicted: $predicted")
      outputFile.println(s"$real,$prediction")
    }

    val realOnes = valueTest.count(_ == 1)
    val predictedOnes = predicted.count(_ == 1)

    println(s"The number of real ones is: $realOnes")
    println(s"The number of ones predicted is: $predictedOnes")
    println("Results with pos_max: " + pos_max + ", c_1: " + c1 + ", c_2: " + c2 + ":")

    val error_out_test = FA.accuracyOfDataset(fileNameTest, weights, nInputs, nHidden, false, numRowsToKeep)
    val error_in = FA.accuracyOfDataset(fileNameTraining, weights, nInputs, nHidden, true, numRowsToKeep)

    println("Ein accuracy: " + error_in)
    println("Eout accuracy: " + error_out_test)
    outputFile.close()
    println(s"Number of features: $nInputs , neurons in the hidden layer: $nHidden")
    println(s"Program execution time: $duration seconds, Ein: $error_in ,  Eout:$error_out_test.")
  }
}
