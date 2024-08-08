package RED

import org.apache.hadoop.shaded.org.apache.commons.math3.optim.nonlinear.vector.Weight
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.util.CollectionAccumulator

import scala.math._
import java.time._
import java.time.format.DateTimeFormatter
//import scala.collection.IterableOnce.iterableOnceExtensionMethods
import scala.collection.mutable.ListBuffer
import scala.io.Source
import scala.util.Random

class Auxiliary_Functions extends Serializable{

  // ReLU activation function
  def relu(x: Double): Double = Math.max(0, x)

  // Sigmoid activation function
  def sigmoid(x: Double): Double = 1.0 / (1.0 + Math.exp(-x))

  def threshold_sign(number: Double, thres: Double): Double ={
    var result: Double = 0.5
    if (number < thres){
      result = 0.0
    }else{
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
    // Transition from the initial to the hidden layer
    for (j <- 0 until nHidden) {
      val weights1 = weights.slice(nInputs * j, nInputs * (j + 1))
      var result = 0.0
      for (k <- 0 until nInputs) {
        result += x(k) * weights1(k)
      }
      z2(j) = tanh(result)
    }
    //Transition from the hidden to the output layer
    var z3 = 0.0
    val weights2 = weights.slice(nInputs * nHidden, weights.length)
    for (k <- 0 until nHidden) {
      z3 += z2(k) * weights2(k)
    }
    // In this acse we dont apply the hypebolic tangent (only the identity function)
    z3
  }

  def forwardPropClas(x: Array[Double], weights: Array[Double], nInputs: Int, nHidden: Int): Double = {
    val z2 = Array.fill(nHidden)(0.0)
    //Transition from the initial to the hidden layer
    for (j <- 0 until nHidden) {
      val weights1 = weights.slice(nInputs * j, nInputs * (j + 1))
      var result = 0.0
      for (k <- 0 until nInputs) {
        result += x(k) * weights1(k)
      }
      z2(j) = relu(result)
    }
    //Transition from the hidden to the output layer
    var z3 = 0.0
    val weights2 = weights.slice(nInputs * nHidden, weights.length)
    for (k <- 0 until nHidden) {
      z3 += z2(k) * weights2(k)
    }
    // In thisd case we apply the hyperbolic tangent to the output layer
    val z4 = sigmoid(z3)
    z4
  }

  def forwardPropClas_2_layers(x: Array[Double], weights: Array[Double], nInputs: Int, nHidden: Int): Double = {
    val z2 = Array.fill(nHidden)(0.0)
    val z3 = Array.fill(nHidden)(0.0)

    // Transition from the initial layer to the first hidden layer
    for (j <- 0 until nHidden) {
      val weights1 = weights.slice(nInputs * j, nInputs * (j + 1))
      var result = 0.0
      for (k <- 0 until nInputs) {
        result += x(k) * weights1(k)
      }
      //z2(j) = tanh(result)
      z2(j) = relu(result)
    }
    //Transition from the first to the second hidden layer
    val decalage = nInputs*nHidden
    for (j <- 0 until nHidden) {
      val weights2 = weights.slice(decalage + nHidden * j, decalage + nHidden * (j + 1))
      var result = 0.0
      for (k <- 0 until nHidden) {
        result += z2(k) * weights2(k)
      }
      //z3(j) = tanh(result)
      z3(j) = relu(result)
    }
    // Trnasition from the hidden to the output layer
    var z4 = 0.0
    val weights3 = weights.slice((nInputs + nHidden) * nHidden, weights.length)
    for (k <- 0 until nHidden) {
      z4 += z3(k) * weights3(k)
    }
    // We apply the hyperbolic tangent to the output layer
    //val z5 = tanh(z4)
    //z5
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

  def calculateF1Score(yTrue: Array[Double], yPred: Array[Double]): Double = {
    require(yTrue.length == yPred.length, "Lists must have the same length")
    val totalSamples = yTrue.length
    // Calculate True Positives, False Positives and False Negatives
    val (tp, fp, fn) = yTrue.zip(yPred).foldLeft((0, 0, 0)) {
      case ((truePositives, falsePositives, falseNegatives), (trueLabel, predictedLabel)) =>
        if (trueLabel == 1.0 && predictedLabel == 1.0) {
          (truePositives + 1, falsePositives, falseNegatives)
        } else if (trueLabel == 0.0 && predictedLabel == 1.0) {
          (truePositives, falsePositives + 1, falseNegatives)
        } else if (trueLabel == 1.0 && predictedLabel == 0.0) {
          (truePositives, falsePositives, falseNegatives + 1)
        } else {
          (truePositives, falsePositives, falseNegatives)
        }
    }
    // Calculate Precision and Recall
    val precision = tp.toDouble / (tp + fp)
    val recall = tp.toDouble / (tp + fn)
    // Calculate F1-score
    val f1Score = 2 * (precision * recall) / (precision + recall)
    f1Score
  }
  def calculateAUCROC(yTrue: Array[Double], yPred: Array[Double]): Double = {
    require(yTrue.length == yPred.length, "Lists must have the same length")
    val sortedIndices = yTrue.indices.sortBy(i => yPred(i)).reverse
    var aucSum = 0.0
    var currentFP = 0
    var currentTP = 0
    var lastFP = 0
    var lastTP = 0
    for (i <- sortedIndices) {
      if (yTrue(i) == 1.0) {
        currentTP += 1
        aucSum += currentFP - lastFP + (currentFP - lastFP + 1) / 2.0
        lastTP = currentTP
      } else {
        currentFP += 1
        lastFP = currentFP
      }
    }
    val totalPositives = yTrue.count(_ == 1.0)
    val totalNegatives = yTrue.length - totalPositives
    val normalizedAUC = aucSum / (totalPositives * totalNegatives).toDouble
    normalizedAUC
  }

  def MSERed(x: Array[Array[Double]], y: Array[Double], weights: Array[Double], nInputs: Int, nHidden: Int): Double = {
    var result = 0.0
    val n_data = x.length
    for (i <- 0 until n_data){
      //val pred = forwardProp(x(i), weights, nInputs, nHidden)
      val pred = forwardPropClas(x(i), weights, nInputs, nHidden)
      result += math.pow(y(i) - pred, 2)
    }
    result /= n_data
    result
  }

  def AccuracyRed(x: Array[Array[Double]], y: Array[Double], weights: Array[Double], nInputs: Int, nHidden: Int, umbral: Double): Double = {
    var result = 0.0
    val n_datos = x.length
    var yPred: Array[Double] = Array.emptyDoubleArray
    for (i <- 0 until n_datos) {
      //val pr = signum(forwardPropClas(x(i), weights, nInputs, nHidden))
      val pr = threshold_sign(forwardPropClas(x(i), weights, nInputs, nHidden), 0.5)
      //If we used a 2 layers net
      //val pr = threshold_sign(forwardPropClas_2_capas(x(i), weights, nInputs, nHidden), treshold)
      yPred = yPred :+ pr
    }
    result = 1.0 - calculateAccuracy(y, yPred)
    //result = calculateAccuracy(y, yPred)
    //println("result accuracy: " + result)
    result

  }

  def binaryEntropyRed(x: Array[Array[Double]], y: Array[Double], weights: Array[Double], nInputs: Int, nHidden: Int): Double = {
    var resul = 0.0
    val n_data = x.length
    var yPred: Array[Double] = Array.emptyDoubleArray
    for (i <- 0 until n_data) {
      val pr = forwardPropClas(x(i), weights, nInputs, nHidden)
      yPred = yPred :+ pr
    }
    resul = binaryCrossEntropy(y, yPred)
    resul
  }

  def f1Red(x: Array[Array[Double]], y: Array[Double], weights: Array[Double], nInputs: Int, nHidden: Int): Double = {
    var result = 0.0
    val n_datos = x.length
    var yPred: Array[Double] = Array.emptyDoubleArray
    for (i <- 0 until n_datos) {
      val pr = signum(forwardPropClas(x(i), weights, nInputs, nHidden))
      yPred = yPred :+ pr
    }
    result = 1.0 - calculateF1Score(y, yPred)
    //println("result: " + result)
    result
  }

  def aucRed(x: Array[Array[Double]], y: Array[Double], weights: Array[Double], nInputs: Int, nHidden: Int): Double = {
    var result = 0.0
    val n_data = x.length
    var yPred: Array[Double] = Array.emptyDoubleArray
    for (i <- 0 until n_data) {
      val pr = signum(forwardPropClas(x(i), weights, nInputs, nHidden))
      yPred = yPred :+ pr
    }
    result = 1.0 - calculateAUCROC(y, yPred)
    //println("result: " + result)
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
  def encode[T](values: List[T]): List[List[Double]] = {
    val uniqueValues = values.distinct
    val numValues = uniqueValues.length
    val numSamples = values.length
    values.map { value =>
      val index = uniqueValues.indexOf(value)
      List.tabulate(numValues)(i => if (i == index) 1.0 else 0.0)
    }
  }
  def encodeSmoker(preData: List[Array[String]]): List[Array[Double]]={
    //Tomar datos a procesar
    val hearingLeft = preData.map(_(7))
    val hearingRight = preData.map(_(8))
    val caries = preData.map(_(22))
    //Data not to be processed
    val dataAux: List[Array[String]] = preData.map { row =>
      row.slice(1, 7)
    }
    val dataAuxDouble: List[Array[Double]] = dataAux.map(row => row.map(_.toDouble))
    val dataAux2: List[Array[String]] = preData.map { row =>
      row.slice(9, 22)
    }
    val dataAuxDouble2: List[Array[Double]] = dataAux2.map(row => row.map(_.toDouble))
    //Transform data
    val oneHotHearingLeft = encode(hearingLeft)
    val oneHotHearingRight = encode(hearingRight)
    val oneHotCaries = encode(caries)
    //Combine the matrices
    val combinedMatrix1 = oneHotHearingLeft.zip(oneHotHearingRight).map { case (rowA, rowB) =>
      rowA ++ rowB
    }
    val combinedMatrix2 = combinedMatrix1.zip(oneHotCaries).map { case (rowA, rowB) =>
      rowA ++ rowB
    }
    val combinedMatrix2Array = combinedMatrix2.map(_.toArray)
    val data1 = combinedMatrix2Array.zip(dataAuxDouble).map { case (rowA, rowB) =>
      rowA ++ rowB
    }
    val data = data1.zip(dataAuxDouble2).map { case (rowA, rowB) =>
      rowA ++ rowB
    }
    data
  }
  //Generates one uniform between -a and a
  def Uniform(a: Double, rand: Random): Double = {
    val num = rand.nextDouble() * 2 * a // generates a random between 0.0 and 2a
    val ret = num - a
    ret
  }

  def fitnessEval(x: Array[Array[Double]], y: Array[Double], particle_weights: Array[Double], nInputs: Int, nHidden: Int): Array[Double] = {
    val nWeights: Int =nHidden*(nInputs + 1)
    //We arer going to use 2 hidden layers
    //val nWeights: Int =nHidden*(nInputs + nHidden + 1)
    if (particle_weights == null) {
      println("The weights array is null")
      return Array.empty[Double]
    }
    val best_fit_local = particle_weights(3 * nWeights)
    val weights = particle_weights.slice(0, nWeights)
    //val fit = MSERed(x, y, weights, nInputs, nHidden)
    //val fit = AccuracyRed(x, y, weights, nInputs, nHidden, umbral)
    //val fit = f1Red(x, y, weights, nInputs, nHidden)
    //val fit = aucRed(x, y, weights, nInputs, nHidden)
    val fit = binaryEntropyRed(x, y, weights, nInputs, nHidden)
    //println("fit " + fit)
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
      if (part(k) > pos_max ){
        part(k) = pos_max
      } else if (part(k) < -pos_max){
        part(k) = -pos_max
      }
      part(N + k) = velocities(k)
    }
    part
  }

  def modifyAccum(part: Array[Double], N: Int, local_accum_pos: CollectionAccumulator[Array[Double]], local_accum_fit: CollectionAccumulator[Double]): Unit = {
    local_accum_pos.add((part.slice(2 * N, 3 * N)))
    local_accum_fit.add(part(3 * N))
  }

  // Convert ther Array[Array[Double]] into ListBuffer[Array[Double]]
  def toListBuffer(arrayArray: Array[Array[Double]]): ListBuffer[Array[Double]] = {
    val listBuffer: ListBuffer[Array[Double]] = ListBuffer.empty[Array[Double]]
    // Traverse each subarray in the Array[Array[Double]] and convert into List[Double]
    for (subArray <- arrayArray) {
      val list: List[Double] = subArray.toList
      // Aggregate List[Double] to ListBuffer[Array[Double]]
      listBuffer += list.toArray
    }

    listBuffer
  }

  def accuracyOfDataset(archivo_val: String, weights: Array[Double], nInputs: Int, nHidden: Int, takeRows: Boolean, numRowsToKeep: Int): Double={
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
      case _ => throw new IllegalArgumentException("Not valid value in List")
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
    //val result = calculateAccuracy(y, yPred)
    result
  }

  def f1OfDataset(archivo_val: String, weights: Array[Double], nInputs: Int, nHidden: Int): Double = {
    var dataRows = Source.fromFile(archivo_val).getLines.drop(1).filter { line =>
      val cols = line.split(",").map(_.trim)
      cols.forall(_.nonEmpty) // Filters rows with non empty values
    }.toList.map { line =>
      val cols = line.split(",").map(_.trim)
      cols
    }
    val dataListString: List[Array[String]] = dataRows.map(_.take(68))
    //val dataTest: List[Array[Double]] = dataListStringTest.map(_.map(_.toDouble))
    var valorStringTest = dataRows.map(_(68))
    val valor: List[Double] = valorStringTest.map {
      case "0" => 0.0
      case "1" => 1.0
      case _ => throw new IllegalArgumentException("Not valid value in list")
    }
    val data = encodeSmoker(dataListString)
    val x = data.toArray
    val y = valor.toArray
    var yPred: Array[Double] = Array.emptyDoubleArray
    for (i <- 0 until x.length) {
      val pr = signum(forwardPropClas(x(i), weights, nInputs, nHidden))
      yPred = yPred :+ pr
    }
    //println("Amount of ones predicted: ")
    val realOnes = y.count(_ == 1)
    val predictedOnes = yPred.count(_ == 1)
    println(s"The amount of real ones is: $realOnes")
    println(s"The amount of predicted ones is: $predictedOnes")
    val result = 1.0 - calculateF1Score(y, yPred)
    result
  }

  def aucOfDataset(file_val: String, weights: Array[Double], nInputs: Int, nHidden: Int): Double = {
    var dataRows = Source.fromFile(file_val).getLines.drop(1).filter { line =>
      val cols = line.split(",").map(_.trim)
      cols.forall(_.nonEmpty) // Filters rows with non empty values
    }.toList.map { line =>
      val cols = line.split(",").map(_.trim)
      cols
    }
    val dataListString: List[Array[String]] = dataRows.map(_.take(23))
    val data: List[Array[Double]] = dataListString.map(_.map(_.toDouble))
    var valorString = dataRows.map(_(23))
    val valor: List[Double] = valorString.map {
      case "0" => 0.0
      case "1" => 1.0
      case _ => throw new IllegalArgumentException("Not valid value in List")
    }

    val x = data.toArray
    val y = valor.toArray

    var yPred: Array[Double] = Array.emptyDoubleArray
    for (i <- 0 until x.length) {
      val pr = signum(forwardPropClas(x(i), weights, nInputs, nHidden))
      yPred = yPred :+ pr
    }

    val result   = 1.0 - calculateAUCROC(y, yPred)
    result
  }


}
