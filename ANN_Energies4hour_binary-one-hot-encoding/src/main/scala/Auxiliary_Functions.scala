import org.apache.spark.util.CollectionAccumulator

import java.time.LocalDate
import java.time.format.DateTimeFormatter
import scala.collection.mutable.ListBuffer
import scala.io.Source
import scala.math.signum
import scala.util.Random
class Auxiliary_Functions {
  // Calculates wT*x(i) (Predicted value for datum x(i))
  def h(x: Array[Array[Double]], w: Array[Double], i: Int): Double = {
    var sum = 0.0
    for (j <- x(i).indices) {
      sum += x(i)(j) * w(j)
    }
    sum
  }

  def relu(x: Double): Double = {
    if (x > 0) {
      x
    } else {
      0.0
    }
  }

  def forwardProp(x: Array[Double], weights: Array[Double], nInputs: Int, nHidden: Int): Double = {
    val z2 = Array.fill(nHidden)(0.0)
    //Change from initial to hidden layer
    for (j <- 0 until nHidden) {
      //println("weights length: " + weights.length)
      val weights1 = weights.slice(nInputs * j, nInputs * (j + 1))
      var result = 0.0
      for (k <- 0 until nInputs) {
        //println("nInputs: " + nInputs)
        result += x(k) * weights1(k)
      }
      z2(j) = result
    }
    //transition from the hidden neuron layer to the output layer
    var z3 = 0.0
    val weights2 = weights.slice(nInputs * nHidden, weights.length)
    for (k <- 0 until nHidden) {
      z3 += z2(k) * weights2(k)
    }
    z3 //it does not apply on the output layer
  }

  def MSERed(x: Array[Array[Double]], y: Array[Double], weights: Array[Double], nInputs: Int, nHidden: Int): Double = {
    var result = 0.0
    val n_data = x.length

    for (i <- 0 until n_data) {
      //println("nInputs: " + nInputs)
      //println("nHidden: " + nHidden)
      val pred = forwardProp(x(i), weights, nInputs, nHidden)
      //println("pred: " + pred)
      result += math.pow(y(i) - pred, 2)
    }
    result /= n_data
    result
  }

  def fitnessEval(x: Array[Array[Double]], y: Array[Double], weights_particle: Array[Double], nInputs: Int, nHidden: Int): Array[Double] = {
    val nWeights: Int = nHidden * (nInputs + 1)
    if (weights_particle == null) {
      println("The weights array is null")
      return Array.empty[Double]
    }
    val best_fit_local = weights_particle(3 * nWeights)
    val weights = weights_particle.slice(0, nWeights)
    val fit = MSERed(x, y, weights, nInputs, nHidden)
    if (fit < best_fit_local) {
      weights_particle(3 * nWeights) = fit
      for (k <- 0 until nWeights) {
        weights_particle(2 * nWeights + k) = weights(k)
      }
    }
    weights_particle
  }

  def posEval(part: Array[Double], mpg: Array[Double], N: Int, rand: Random, W: Double, c_1: Double, c_2: Double, V_max: Double, pos_max: Double): Array[Double] = {
    val velocities = part.slice(N, 2 * N)
    val mpl = part.slice(2 * N, 3 * N)
    val r_1 = rand.nextDouble()
    val r_2 = rand.nextDouble()
    for (k <- 0 until N) {
      velocities(k) = W * velocities(k) + c_1 * r_1 * (mpl(k) - part(k)) + c_2 * r_2 * (mpl(k) - part(k))
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

  def modifyAccum(part: Array[Double], N: Int, local_accum_pos: CollectionAccumulator[Array[Double]], local_accum_fit: CollectionAccumulator[Double]): Unit = {
    local_accum_pos.add((part.slice(2 * N, 3 * N)))
    local_accum_fit.add(part(3 * N))
  }

  // Convert Array[Array[Double]] to a ListBuffer[Array[Double]]
  def toListBuffer(arrayArray: Array[Array[Double]]): ListBuffer[Array[Double]] = {
    val listBuffer: ListBuffer[Array[Double]] = ListBuffer.empty[Array[Double]]

    // Go to each subarray in the Array[Array[Double]] and convert it to a List[Double].
    for (subArray <- arrayArray) {
      val list: List[Double] = subArray.toList
      // Add List[Double] to ListBuffer[Array[Double]]
      listBuffer += list.toArray
    }
    listBuffer
  }

  //Operations that have to do with the energy/time consumption data format
  def convertToDayOfWeek(dates: List[String]): List[String] = {
    val formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd")
    dates.map { dateStr =>
      val date = LocalDate.parse(dateStr, formatter)
      val dayOfWeek = date.getDayOfWeek.toString
      dayOfWeek
    }
  }
  def convertToMonthOfYear(dates: List[String]): List[String] = {
    val formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd")
    dates.map { dateStr =>
      val date = LocalDate.parse(dateStr, formatter)
      val monthOfYear = date.getMonth.toString
      monthOfYear
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

  //Generate a random number between -a and a
  def Uniform(a: Double, rand: Random): Double = {
    val num = rand.nextDouble() * 2 * a // generates a random number between 0.0 and 2a
    val ret = num - a
    ret
  }

  ///Hot encoding functions
  def encode[T](values: List[T]): List[List[Double]] = {
    val uniqueValues = values.distinct
    val numValues = uniqueValues.length
    val numSamples = values.length

    values.map { value =>
      val index = uniqueValues.indexOf(value)
      List.tabulate(numValues)(i => if (i == index) 1.0 else 0.0)
    }
  }

  ///MSE calculations
  def MSEOfDataSeparated(dataSeparated: Array[List[Array[Double]]], valorSeparated: Array[List[Double]], arrayWeights: Array[Array[Double]], nInputs: Int, nHidden: Int): Double = {
    var result = 0.0
    for (hour <- 0 until 24){
      val x = dataSeparated(hour).toArray
      val y = valorSeparated(hour).toArray
      val aux: Double = MSERed(x, y, arrayWeights(hour), nInputs, nHidden)
      result += aux
    }
    result = result / 24.0
    result
  }
  def MSEOfData(pred: List[Double], valor: List[Double]): Double = {
    var result = 0.0
    for (i <- 0 until pred.length)
    result += math.pow(valor(i) - pred(i), 2)

    result = result / pred.length
    result
  }
}
