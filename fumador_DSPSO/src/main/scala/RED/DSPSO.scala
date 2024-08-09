package RED

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.util.CollectionAccumulator
import org.apache.spark.util.DoubleAccumulator
import scala.util.Random
import java.io._
import scala.io.Source
import org.apache.spark.util.CollectionAccumulator
import scala.math._
import java.time._
import java.time.format.DateTimeFormatter
import scala.collection.mutable.ListBuffer
import scala.util.Random
/**
 * @author ${user.name}
 */


object App {
  val conf = new SparkConf()
    .setAppName("DSPSO")
    .setMaster("local[*]")
    //.set("spark.rapids.sql.concurrentGpuTasks", "1")
    //.set("spark.executor.resource.gpu.amount", "1")
    //.set("spark.executor.resource.gpu.discoveryScript", "/opt/sparkRapidsPlugin/getGpusResources.sh")
    //.set("spark.executor.memory", "4g")
    //.set("spark.driver.host", "192.168.1.176")
    //.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    //.set("spark.plugins", "com.nvidia.spark.SQLPlugin")
  val sc = SparkContext.getOrCreate(conf)
  var particles = Array.empty[Array[Double]]
  var best_global_pos = Array.empty[Double]
  var best_global_fitness = Double.MaxValue
  val rand = new Random
  val pos_max = 1.0
  val W = 1.0
  val c1 = 3.5
  val c2 = 1.8
  val V_max = 0.1*pos_max
  val I = 100
  val m = 100

  // ReLU activation function
  def relu(x: Double): Double = Math.max(0, x)

  // Sigmoid activation function
  def sigmoid(x: Double): Double = 1.0 / (1.0 + Math.exp(-x))

  def threshold_sign(number: Double, tresh: Double): Double = {
    var result: Double = 0.5
    if (number < tresh) {
      result = 0.0
    } else {
      result = 1.0
    }
    result
  }

  // Calculates wT*x(i) (predicted value for datum x(i))
  def h(x: Array[Array[Double]], w: Array[Double], i: Int): Double = {
    var sum = 0.0
    for (j <- x(i).indices) {
      sum += x(i)(j) * w(j)
    }
    sum
  }

  def forwardProp(x: Array[Double], weights: Array[Double], nInputs: Int, nHidden: Int): Double = {
    val z2 = Array.fill(nHidden)(0.0)
    //Transition from the initial to the hidden layer
    for (j <- 0 until nHidden) {
      val weights1 = weights.slice(nInputs * j, nInputs * (j + 1))
      var result = 0.0
      for (k <- 0 until nInputs) {
        result += x(k) * weights1(k)
      }
      z2(j) = result
    }
    //Trsnsition from the hidden to the output layer
    var z3 = 0.0
    val weights2 = weights.slice(nInputs * nHidden, weights.length)
    for (k <- 0 until nHidden) {
      z3 += z2(k) * weights2(k)
    }
    z3 //we don't apply tanh in this case
  }

  def forwardPropClas(x: Array[Double], weights: Array[Double], nInputs: Int, nHidden: Int): Double = {
    val z2 = Array.fill(nHidden)(0.0)

    // Transition from the initial layer to the hidden layer
    for (j <- 0 until nHidden) {
      val weights1 = weights.slice(nInputs * j, nInputs * (j + 1))
      var result = 0.0
      for (k <- 0 until nInputs) {
        result += x(k) * weights1(k)
      }
      z2(j) = relu(result)
    }

    // Transition from the hidden to the output layer
    var z3 = 0.0
    val weights2 = weights.slice(nInputs * nHidden, weights.length)
    for (k <- 0 until nHidden) {
      z3 += z2(k) * weights2(k)
    }
    // We apply the hyperbolic tangent in the output layer (identity function)
    val z4 = sigmoid(z3)
    z4
  }

  def calculateAccuracy(yTrue: Array[Double], yPred: Array[Double]): Double = {
    require(yTrue.length == yPred.length, "Lists must have the same length")
    val totalSamples = yTrue.length
    val correctPredictions = yTrue.zip(yPred).count { case (trueLabel, predictedLabel) => trueLabel == predictedLabel }
    val result = correctPredictions.toDouble / totalSamples
    //println("result accuracy en calculateAccuracy: " + result)
    result
  }

  def binaryCrossEntropy(yTrue: Array[Double], yPred: Array[Double]): Double = {
    require(yTrue.length == yPred.length, "Lists must have the same length")

    val epsilon = 1e-15 // small constant to avoid 0 division
    val clippedYPred = yPred.map(p => Math.max(epsilon, Math.min(1 - epsilon, p))) // Clip to avoid log(0) and log(1)

    val loss = -yTrue.zip(clippedYPred).map { case (trueLabel, predLabel) =>
      trueLabel * Math.log(predLabel) + (1 - trueLabel) * Math.log(1 - predLabel)
    }.sum

    loss
  }

  def binaryEntropyRed(x: Array[Array[Double]], y: Array[Double], weights: Array[Double], nInputs: Int, nHidden: Int): Double = {
    var result = 0.0
    val n_data = x.length

    var yPred: Array[Double] = Array.emptyDoubleArray
    for (i <- 0 until n_data) {
      val pr = forwardPropClas(x(i), weights, nInputs, nHidden)
      yPred = yPred :+ pr
    }
    result = binaryCrossEntropy(y, yPred)
    result

  }

  def MSERed(x: Array[Array[Double]], y: Array[Double], weights: Array[Double], nInputs: Int, nHidden: Int): Double = {
    var result = 0.0
    val n_data = x.length

    for (i <- 0 until n_data) {
      val pred = forwardProp(x(i), weights, nInputs, nHidden)
      result += math.pow(y(i) - pred, 2)
    }
    result /= n_data
    result
  }


  def convertToDayOfWeek(dates: List[String]): List[String] = {
    val formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd")
    dates.map { dateStr =>
      val date = LocalDate.parse(dateStr, formatter)
      val dayOfWeek = date.getDayOfWeek.toString
      dayOfWeek
    }
  }

  def separateHourMinute(hourColumn: List[String]): (List[String], List[String]) = {
    val (hours, minutes) = hourColumn.map { hourStr =>
      val parts = hourStr.split(":")
      (parts(0), parts(1))
    }.unzip
    (hours, minutes)
  }

  def separateDayHourMinuteSecond(dates: List[String]): (List[String], List[String]) = {
    val (days, hours) = dates.map { hourStr =>
      val parts = hourStr.split(" ")
      (parts(0), parts(1))
    }.unzip
    (days, hours)
  }

  def encode[T](values: List[T]): List[List[Double]] = {
    val uniqueValues = values.distinct
    val numValues = uniqueValues.length
    val numSamples = values.length
    values.map { value =>
      val index = uniqueValues.indexOf(value)
      List.tabulate(numValues)(i => if (i == index) 1.0 else 0.0)
    }
  }

  //Generates one uniform between -a and a
  def Uniform(a: Double, rand: Random): Double = {
    val num = rand.nextDouble() * 2 * a // generates one random between 0.0 and 2a
    val ret = num - a
    ret
  }

  def fitnessEval(x: Array[Array[Double]], y: Array[Double], particle_weights: Array[Double], nInputs: Int, nHidden: Int): Array[Double] = {
    val nWeights: Int = nHidden * (nInputs + 1)
    //We are going to use two hidden layers
    //val nWeights: Int =nHidden*(nInputs + nHidden + 1)
    if (particle_weights == null) {
      println("The weights array is nulll")
      return Array.empty[Double]
    }

    val best_fit_local = particle_weights(3 * nWeights)
    val weights = particle_weights.slice(0, nWeights)
    //val fit = MSERed(x, y, weights, nInputs, nHidden)
    //val fit = AccuracyRed(x, y, weights, nInputs, nHidden, umbral)
    //val fit = f1Red(x, y, weights, nInputs, nHidden)
    //val fit = aucRed(x, y, weights, nInputs, nHidden)
    val fit = binaryEntropyRed(x, y, weights, nInputs, nHidden)
    if (fit < best_fit_local) {
      particle_weights(3 * nWeights) = fit
      for (k <- 0 until nWeights) {
        particle_weights(2 * nWeights + k) = weights(k)
      }
    }
    particle_weights
  }

  def posEval(part: Array[Double], mpg: Array[Double], N: Int, rand: Random, W: Double, c_1: Double, c_2: Double, V_max: Double, pos_max: Double): Array[Double] = {
    // global ind (not necessary in Scala)
    val velocities = part.slice(N, 2 * N)
    val mpl = part.slice(2 * N, 3 * N)
    val r_1 = rand.nextDouble()
    val r_2 = rand.nextDouble()
    for (k <- 0 until N) {
      velocities(k) = W * velocities(k) + c_1 * r_1 * (mpl(k) - part(k)) + c_2 * r_2 * (mpg(k) - part(k))
      if (velocities(k) > V_max) {
        velocities(k) = V_max
      } else if (velocities(k) < -V_max) {
        velocities(k) = -V_max
      }
      part(k) = part(k) + velocities(k)
      if (part(k) > pos_max) {
        part(k) = pos_max
      } else if (part(k) < -pos_max) {
        part(k) = -pos_max
      }
      part(N + k) = velocities(k)
    }
    part
  }

  // Convert the Array[Array[Double]] into ListBuffer[Array[Double]]
  def toListBuffer(arrayArray: Array[Array[Double]]): ListBuffer[Array[Double]] = {
    val listBuffer: ListBuffer[Array[Double]] = ListBuffer.empty[Array[Double]]

    // Traverse each array in Array[Array[Double]] and convert it into List[Double]
    for (subArray <- arrayArray) {
      val list: List[Double] = subArray.toList
      // Agreggate List[Double] to ListBuffer[Array[Double]]
      listBuffer += list.toArray
    }
    listBuffer
  }

  def accuracyOfDataset(archivo_val: String, weights: Array[Double], nInputs: Int, nHidden: Int, takeRows: Boolean, numRowsToKeep: Int): Double = {
    var dataRows = Source.fromFile(archivo_val).getLines.drop(1).filter { line =>
      val cols = line.split(",").map(_.trim)
      cols.forall(_.nonEmpty) // Filters the rows with non empty values
    }.toList.map { line =>
      val cols = line.split(",").map(_.trim)
      cols
    }
    val dataListString: List[Array[String]] = dataRows.map(_.take(68))
    //val dataTest: List[Array[Double]] = dataListStringTest.map(_.map(_.toDouble))
    var valueStringTest = dataRows.map(_(68))
    var value: List[Double] = valueStringTest.map {
      case "0" => 0.0
      case "1" => 1.0
      case _ => throw new IllegalArgumentException("Not valid value in the list")
    }
    //var data = encodeSmoker(dataListString)
    var data = dataListString.map(_.map(_.toDouble))
    if (takeRows) {
      data = data.take(numRowsToKeep)
      value = value.take(numRowsToKeep)
    }
    val x = data.toArray
    val y = value.toArray
    var yPred: Array[Double] = Array.emptyDoubleArray
    for (i <- 0 until x.length) {
      val pr = threshold_sign(forwardPropClas(x(i), weights, nInputs, nHidden), 0.5)
      yPred = yPred :+ pr
    }
    val result = 1.0 - calculateAccuracy(y, yPred)
    result
  }

  def modifyAccum(part: Array[Double], N: Int, local_accum_pos: CollectionAccumulator[Array[Double]], local_accum_fit: CollectionAccumulator[Double]): Unit = {
    local_accum_pos.add((part.slice(2 * N, 3 * N)))
    local_accum_fit.add(part(3 * N))
  }

  def main(args: Array[String]): Unit = {
    //Fiules for the graphic plots
    val fileNameTraining = "training_feature.csv"
    val fileNameTest = "test_feature.csv"
    val numRowsToKeep: Int = 1000 // Number of rows from the datasets

    var dataRows = Source.fromFile(fileNameTraining).getLines.drop(1).filter { line =>
      val cols = line.split(",").map(_.trim)
      cols.forall(_.nonEmpty) // Filters the rows with non empty values
    }.take(numRowsToKeep).toList.map { line =>
      val cols = line.split(",").map(_.trim)
      cols
    }

    val dataListString: List[Array[String]] = dataRows.map(_.take(68))
    var valueString = dataRows.map(_(68))
    val value: List[Double] = valueString.map {
      case "0" => 0.0
      case "1" => 1.0
      case _ => throw new IllegalArgumentException("Valor no válido en la lista")
    }
    //dataRows.foreach(row => println(row.mkString(", ")))
    val rows = dataRows.length
    val columns = if (rows > 0) dataRows.head.length else 0
    println(s"The number of rows is: $rows")
    println(s"The number of columns is: $columns")

    //Similar processing for test dataset
    var dataRowsTest = Source.fromFile(fileNameTest).getLines.drop(1).filter { line =>
      val cols = line.split(",").map(_.trim)
      cols.forall(_.nonEmpty) // Filters the rows with non empty values
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
    val nHidden: Int = (1.9 * nInputs).toInt
    val nWeights: Int = nHidden * (nInputs + 1)
    val existingFile = new FileWriter(s"smoker_results_$I _$m _$pos_max _$c1 _$c2.txt", true)
    val outputFile = new PrintWriter(existingFile)

    particles = Array.empty[Array[Double]]
    best_global_pos = Array.empty[Double]
    best_global_fitness = Double.MaxValue

    val xSer: Array[Array[Double]] = data.toArray
    val ySer: Array[Double] = value.toArray

    for (i <- 0 until m) {
      val posicion = Array.fill(nWeights)(Uniform(pos_max, rand))
      val velocidad = Array.fill(nWeights)(Uniform(pos_max, rand))
      val fit = binaryEntropyRed(xSer, ySer, posicion, nInputs, nHidden)
      val part_ = posicion ++ velocidad ++ posicion ++ Array(fit)
      if (fit < best_global_fitness) {
        best_global_fitness = fit
        best_global_pos = posicion
      }
      particles = particles :+ part_
    }

    val start = System.nanoTime()
    var rdd_master = sc.parallelize(particles)

    for (i <- 0 until I) {
      println("iteration: " + i)
      val local_accum_pos: CollectionAccumulator[Array[Double]] = sc.collectionAccumulator[Array[Double]]("BestLOcalPos")
      val rdd_fitness = rdd_master.map(part => fitnessEval(xSer, ySer, part, nInputs, nHidden))
      //val local_accum_fit: DoubleAccumulator = sc.doubleAccumulator("My_Accumulator")
      val local_accum_fit: CollectionAccumulator[Double] = sc.collectionAccumulator[Double]("BestLocalFit")
      rdd_fitness.foreach(part => modifyAccum(part, nWeights, local_accum_pos, local_accum_fit))
      //rdd_fitness.collect().foreach(part => modifyAccum(part, nWeights, local_accum_pos, local_accum_fit))

      val blfs = local_accum_fit.value
      //We try to resolve the error
      //val auxError = rdd_fitness.collect()
      for (j <- 0 until m) {
        val blf = blfs.get(j)
        //val blf = auxError(j)(3 * nWeights)
        if (blf < best_global_fitness) {
          best_global_fitness = blf
          best_global_pos = local_accum_pos.value.get(j)
          //best_global_pos = auxError(j).slice(2 * nWeights, 3 * nWeights)
        }
      }
      val result2 = rdd_fitness.map(part => posEval(part, best_global_pos, nWeights, rand, W, c1, c2, V_max, pos_max))
      val result_collected = result2.collect()
      rdd_master = sc.parallelize(result_collected)
    }

    val end = System.nanoTime()
    val duration = (end - start) / 1e9

    val weights = best_global_pos

    var predicted: List[Double] = List()
    for (i <- 0 until dataTest.length) {
      val prDouble = forwardPropClas(dataTest(i), weights, nInputs, nHidden)
      val pr = threshold_sign(prDouble, 0.5)
      predicted = predicted :+ pr
    }

    for ((real, prediction) <- valueTest.zip(predicted)) {
      //println(s"Real: $real - Prediction: $prediction")
      outputFile.println(s"$real,$prediction")
    }

    val realOnes = valueTest.count(_ == 1)
    val predictedOnes = predicted.count(_ == 1)
    println(s"The amount of real ones is: $realOnes")
    println(s"The amount of predicted ones is: $predictedOnes")
    println("Results with pos_max: " + pos_max + ", c_1: " + c1 + ", c_2: " + c2 + ":")
    val error_out_test = accuracyOfDataset(fileNameTest, weights, nInputs, nHidden, false, numRowsToKeep)
    val error_in = accuracyOfDataset(fileNameTraining, weights, nInputs, nHidden, true, numRowsToKeep)
    println("Ein accuracy: " + error_in)
    println("Eout accuracy: " + error_out_test)
    outputFile.close()
    //println(s"$I,$m")
    println(s"Program execution time $duration seconds.")
  }
}


