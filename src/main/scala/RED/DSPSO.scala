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
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

/**
 * @author Manuel I. Capel
 */


object App {

  val conf = new SparkConf()
    .setAppName("Distributed Synchronous PSO-DSPSO ")
    .setMaster("local[*]")
  val sc = SparkContext.getOrCreate(conf)

  // NUmber of particles
  val m = 100
  // Number of iterations
  val I = 10

  var particles = Array.empty[Array[Double]]
  var best_global_position = Array.empty[Double]
  var best_global_fitness = Double.MaxValue

  val rand = new Random

  val W = 1.0
  val c_1 = 0.8
  val c_2 = 0.2
  val V_max = 10.0

  // Calculates wT*x(i) (Predicted Value for datum x(i)); wT is the ANN weights vector
  def h(x: Array[Array[Double]], w: Array[Double], i: Int): Double = {
    var sum = 0.0
    for (j <- x(i).indices) {
      sum += x(i)(j) * w(j)
    }
    sum
  }

  def forwardProp(x: Array[Double], weights: Array[Double], nInputs: Int, nHidden: Int): Double = {
    val z2 = Array.fill(nHidden)(0.0)
    //Transfer from the initial layer to the hidden layer
    for (j <- 0 until nHidden) {
      val weights1 = weights.slice(nInputs * j, nInputs * (j + 1))
      var result = 0.0
      for (k <- 0 until nInputs) {
        result += x(k) * weights1(k)
      }
      z2(j) = tanh(result)
    }
    //transfer from the hidden neuron layer to the output layer
    var z3 = 0.0
    val weights2 = weights.slice(nInputs * nHidden, weights.length)
    for (k <- 0 until nHidden) {
      z3 += z2(k) * weights2(k)
    }
    z3 //the tanh() function is not applied in the output layer
  }

  def MSERed(x: Array[Array[Double]], y: Array[Double], weights: Array[Double], nInputs: Int, nHidden: Int): Double = {
    var result = 0.0
    val num_data = x.length

    for (i <- 0 until num_data) {
      val pred = forwardProp(x(i), weights, nInputs, nHidden)
      result += math.pow(y(i) - pred, 2)
    }
    result /= num_data
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

  //Generates a 'uniform' value between -a and a
  def Uniform(a: Double, rand: Random): Double = {
    val num = rand.nextDouble() * 2 * a // Generates a random number in teh interval [0.0.. 2a]
    val ret = num - a
    ret
  }

  def fitnessEval(x: Array[Array[Double]], y: Array[Double], weights_particle: Array[Double], nInputs: Int, nHidden: Int): Array[Double] = {

    val nWeights: Int = nHidden*(nInputs+1)

    if (weights_particle == null) {
      println("El array de pesos es null")
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

  def posEval(part: Array[Double], mpg: Array[Double], N: Int, rand: Random, W: Double, c_1: Double, c_2: Double, V_max: Double): Array[Double] = {
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
      part(N + k) = velocities(k)
    }
    part
  }

  def modifyAccum(part: Array[Double], N: Int, local_accum_pos: CollectionAccumulator[Array[Double]], local_accum_fit: CollectionAccumulator[Double]): Unit = {
    local_accum_pos.add((part.slice(2 * N, 3 * N)))
    local_accum_fit.add(part(3 * N))
  }

  def main(args: Array[String]): Unit = {
    val fileName = "demanda_limpia_2020.csv"

    val numRowsToKeep: Int = 1200 // Number of rows to keep

    var dataRows = Source.fromFile(fileName).getLines.drop(1).filter { line =>
      val cols = line.split(",").map(_.trim)
      cols.forall(_.nonEmpty) // Filter rows with non-empty values
    }.take(numRowsToKeep).toList.map { line =>
      val cols = line.split(",").map(_.trim)
      cols
    }
    val dates = dataRows.map(_(4))
    val potReal = dataRows.map(_(1)).map(_.toDouble)
    val potProgramada = dataRows.map(_(3)).map(_.toDouble)
    //////
    val (days, hours) = separateDayHourMinuteSecond(dates)
    val daysOfWeek = convertToDayOfWeek(days)
    var (h, mi) = separateHourMinute(hours)
    val oneHotHours = encode(h)
    val oneHotMinutes = encode(mi)
    val oneHotDays = encode(daysOfWeek)
    val combinedMatrix1 = oneHotHours.zip(oneHotDays).map { case (rowA, rowB) => rowA ++ rowB }
    val combinedMatrix2 = combinedMatrix1.zip(oneHotMinutes).map { case (rowA, rowB) => rowA ++ rowB }
    val dataList = combinedMatrix2.zip(potProgramada).map { case (row, value) => row :+ value }
    val data: List[Array[Double]] = dataList.map(_.toArray)
    /////
    val separatedData: Array[List[Array[Double]]] = Array.fill(24)(List.empty)
    val separatedPotReal: Array[List[Double]] = Array.fill(24)(List.empty)
    val separatedPotProgramada: Array[List[Double]] = Array.fill(24)(List.empty)
    for ((array, index) <- data.zipWithIndex) {
      for (hour <- 0 until 24) {
        if (array(hour) == 1.0) {
          separatedData(hour) = array.slice(24, array.length) :: separatedData(hour)
          separatedPotProgramada(hour) = potProgramada(index) :: separatedPotProgramada(hour)
          separatedPotReal(hour) = potReal(index) :: separatedPotReal(hour)
        }
      }
    }
    ///////////
    val nInputs: Int = separatedData(0).headOption.map(_.size).getOrElse(0)
    val nHidden: Int = (1.9 * nInputs).toInt
    val arrayWeights: Array[Array[Double]] = Array.fill(24)(Array.empty[Double])
    val nWeights: Int = nHidden * (nInputs + 1)
    val n = nWeights
    //////////
    val start = System.nanoTime()
    //Execution of the DSPSO (Distributed Synchronous PSO) variant of the PSO algorithm
    for (hour <- 0 until 24) {
      particles = Array.empty[Array[Double]]
      best_global_position = Array.empty[Double]
      best_global_fitness = Double.MaxValue
      // Convert the lists to serializable arrays
      val xSer: Array[Array[Double]] = separatedData(hour).toArray
      val ySer: Array[Double] = separatedPotReal(hour).toArray
      // Initializing the vectors
      for (i <- 0 until m) {
        //Here it may be better to initialize the last positions to smaller values
        val position = Array.fill(nWeights)(Uniform(100, rand))
        val velocity = Array.fill(nWeights)(Uniform(100, rand))
        val fit = MSERed(xSer, ySer, position, nInputs, nHidden)
        val part_ = position ++ velocity ++ position ++ Array(fit)
        if (fit < best_global_fitness) {
          best_global_fitness = fit
          best_global_position = position
        }
        particles = particles :+ part_
      }
      //Process
      var rdd_master = sc.parallelize(particles)

      for (i <- 0 until I) {
        val local_accum_pos: CollectionAccumulator[Array[Double]] = sc.collectionAccumulator[Array[Double]]("BestLocalPositions")
        //val local_accum_fit: DoubleAccumulator = sc.doubleAccumulator("MiAcumulador")
        val local_accum_fit: CollectionAccumulator[Double] = sc.collectionAccumulator[Double]("BestLocalFits")
        val rdd_fitness = rdd_master.map(part => fitnessEval(xSer, ySer, part, nInputs, nHidden))
        rdd_fitness.foreach(part => modifyAccum(part, nWeights, local_accum_pos, local_accum_fit))
        val blfs = local_accum_fit.value
        for (j <- 0 until m) {
          val blf = blfs.get(j)
          if (blf < best_global_fitness) {
            best_global_fitness = blf
            best_global_position = local_accum_pos.value.get(j)
          }
        }
        val result2 = rdd_fitness.map(part => posEval(part, best_global_position, nWeights, rand, W, c_1, c_2, V_max))
        val result_collected = result2.collect()
        rdd_master = sc.parallelize(result_collected)
      }
      arrayWeights(hour) = best_global_position
    }
    val end = System.nanoTime()
    ////////////////
    val keyValueMap: Map[Int, String] = Map(
      0 -> "00:00",
      1 -> "01:00",
      2 -> "02:00",
      3 -> "03:00",
      4 -> "04:00",
      5 -> "05:00",
      6 -> "06:00",
      7 -> "07:00",
      8 -> "08:00",
      9 -> "09:00",
      10 -> "10:00",
      11 -> "11:00",
      12 -> "12:00",
      13 -> "13:00",
      14 -> "14:00",
      15 -> "15:00",
      16 -> "16:00",
      17 -> "17:00",
      18 -> "18:00",
      19 -> "19:00",
      20 -> "20:00",
      21 -> "21:00",
      22 -> "22:00",
      23 -> "23:00",
    )
    ////////////////
    var predictedPower: Array[List[Double]] = Array.fill(24)(List.empty[Double])
    //Predicción
    for (hour <- 0 until 24) {
      for (i <- 0 until separatedData(hour).length) {
        val pot = forwardProp(separatedData(hour)(i), arrayWeights(hour), nInputs, nHidden)
                predictedPower(hour) = predictedPower(hour) :+ pot
      }
    }
    //results
    val filePath="output.txt"
    val writer= new FileWriter(filePath,true)
    val currentDateTime = LocalDateTime.now()
    val formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")
    val formattedDateTime = currentDateTime.format(formatter)
    writer.write(formattedDateTime)
    writer.write("\n")
    for (hour <- 0 until 24) {
      //println("results for " + keyValueMap(hour))
      writer.write("results for " + keyValueMap(hour))
      //println("Weights for " + keyValueMap(hour) + ": " + arrayWeights(hour).mkString(", "))
      writer.write("\n")
      writer.write("Weights for " + keyValueMap(hour) + ": " + arrayWeights(hour).mkString(", "))
      writer.write("\n")
      for ((real, predicted) <- separatedPotReal(hour).zip(predictedPower(hour))) {
       //println(s"Electric prower real: $real - Electric power predicted: $predicted")
        writer.write(s"Electric prower real: $real - Electric power predicted: $predicted")
      }
      writer.write("\n")
      writer.write("\n")
    }
    writer.close()
    val time = (end - start) / 1e9
    println(s"Execution time(s):$time")

    //testing
    println("weights: ")
    for (hour <- 0 until 24) {
       println(arrayWeights(hour).mkString(", "))
   }
  }
}


