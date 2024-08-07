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
import java.util.Locale
import java.text.DecimalFormat
import java.text.DecimalFormatSymbols
/**
 * @author Manuel I. Capel
 */
object App {
  val conf = new SparkConf()
    .setAppName("Distributed Synchronous PSO-DSPSO ")
    .setMaster("local[*]")
    //.setMaster("spark://localhost:7077")
   // .set("spark.executor.resource.gpu.amount", "1")
   // .set("spark.executor.memory", "20G")
   // .setJars(Array("/path-to-the-jar-file/red_neuronal_DSPSO-assembly-0.1.0-SNAPSHOT.jar"))
    //.set("spark.eventLog.dir", "/opt/spark-3.5.0-bin-hadoop3/logs")
    //.set("spark.eventLog.enabled", "true")
    //.set("spark.executor.resource.gpu.discoveryScript", "/opt/sparkRapidsPlugin/getGpusResources.sh")
    //SparkRapids only works with Ubuntu 20.04
    .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    .set("spark.sql.hive.convertMetastoreParquet", "false")
    .set("spark.rdd.compress", "true")
  val sc = SparkContext.getOrCreate(conf)

  // NUmber of particles
  val m = 100
  // Number of iterations
  val I = 100

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

  //The function is primarily responsible for updating the weights and fitness of a particle if the new weights
  // yield better performance. It leverages a neural network's fitness (the Mean Squared Error) to determine
  // if an update is necessary.
  def fitnessEval(x: Array[Array[Double]], y: Array[Double], weights_particle: Array[Double], nInputs: Int, nHidden: Int): Array[Double] = {
    //Example Layout, If nWeights is 10, then:
    //weights_particle(0) to weights_particle(9): Current weights of the particle.
    //weights_particle(10) to weights_particle(19): Best-known weights found by this particle (personal best).
    //weights_particle(20) to weights_particle(29): Additional weights of velocity of the particle.
    //weights_particle(30): Best fitness value associated with the personal best weights
    // Validate inputs
    if (x == null || y == null) {
      throw new IllegalArgumentException("Input arrays x and y cannot be null")
    }
    if (x.length == 0 || y.length == 0) {
      throw new IllegalArgumentException("Input arrays x and y cannot be empty")
    }
    if (x.exists(_.length != nInputs)) {
      throw new IllegalArgumentException(s"Each row in x must have $nInputs elements")
    }
    if (x.length != y.length) {
      throw new IllegalArgumentException("Input arrays x and y must have the same number of elements")
    }
    val nWeights: Int = nHidden*(nInputs+1)
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

  def posEval(part: Array[Double], mpg: Array[Double], N: Int, rand: Random, W: Double, c_1: Double, c_2: Double, V_max: Double): Array[Double] = {
    // global ind (not necessary in Scala)
    // Check if the length of 'part' is at least 3 * N.part array has at least 3 * N elements, as the
    // function slices it into three parts (positions, velocities, and mpl).
    if (part.length < 3 * N) {
      throw new IllegalArgumentException(s"part array length is ${part.length}, but it must be at least ${3 * N}")
    }
    // Check if the length of 'mpg' is at least N
    if (mpg.length < N) {
      throw new IllegalArgumentException(s"mpg array length is ${mpg.length}, but it must be at least $N")
    }
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
    //local_accum_pos: A CollectionAccumulator that accumulates arrays of doubles,
    // to store the local best positions of particles.
    //local_accum_fit: A CollectionAccumulator that accumulates doubles,
    // to store the fitness values of particles.
    if (part.length < 3 * N + 1) {// Validate that part has the expected size
      throw new IllegalArgumentException(s"part array length is ${part.length}, but it must be at least ${3 * N + 1}")
    }
    if (local_accum_pos == null) {     // Validate that accumulators are not null
      throw new IllegalArgumentException("local_accum_pos accumulator is null")
    }
    if (local_accum_fit == null) {      // Validate that accumulators are not null
      throw new IllegalArgumentException("local_accum_fit accumulator is null")
    }
    // Add the local best position to the position accumulator
    local_accum_pos.add((part.slice(2 * N, 3 * N)))
    // Add the fitness value to the fitness accumulator
    local_accum_fit.add(part(3 * N))
  }

  def main(args: Array[String]): Unit = {
    //val fileName = "demanda_limpia_2020.csv"
    val fileName = "demanda_limpia_final.csv"
    //val numRowsToKeep: Int =175104  // Number of rows to keep
   // val numRowsToKeep: Int =12000
    val numRowsToKeep: Int =30240

    var dataRows = Source.fromFile(fileName).getLines.drop(1).filter { line =>
      val cols = line.split(",").map(_.trim)
      cols.forall(_.nonEmpty) // Filter rows with non-empty values
    }.take(numRowsToKeep).toList.map { line =>
      val cols = line.split(",").map(_.trim)
      cols
    }
    //The result is stored in dataRows, which is a List[Array[String]]. Each element of
    // this list corresponds to a row in the CSV, and each row is represented as an array
    // of strings (one for each column).
    val dates = dataRows.map(_(4))
    val realPow = dataRows.map(_(1)).map(_.toDouble)
    val programmedPow = dataRows.map(_(3)).map(_.toDouble)
    val (days, hours) = separateDayHourMinuteSecond(dates)
    val daysOfWeek = convertToDayOfWeek(days)
    var (h, mi) = separateHourMinute(hours)
    val oneHotHours = encode(h)
    val oneHotMinutes = encode(mi)
    val oneHotDays = encode(daysOfWeek)
    val combinedMatrix1 = oneHotHours.zip(oneHotDays).map { case (rowA, rowB) => rowA ++ rowB }
    val combinedMatrix2 = combinedMatrix1.zip(oneHotMinutes).map { case (rowA, rowB) => rowA ++ rowB }
    val dataList = combinedMatrix2.zip(programmedPow).map { case (row, value) => row :+ value }
    /////The final output data is a List[Array[Double]], which can be fed into a machine learning model
    // for training or testing.
    val data: List[Array[Double]] = dataList.map(_.toArray)

    //The following code snippet effectively organizes data by hour based on
    // one-hot encoded hour information, allowing you to analyze or process data specific
    // to each hour of the day.
    // The approach is flexible, but is needed fine tuning to ensure data integrity and performance.
    val separatedData: Array[List[Array[Double]]] = Array.fill(24)(List.empty)
    val separatedPotReal: Array[List[Double]] = Array.fill(24)(List.empty)
    val separatedPotProgramada: Array[List[Double]] = Array.fill(24)(List.empty)
    for ((array, index) <- data.zipWithIndex) {
      for (hour <- 0 until 24) {
        if (array(hour) == 1.0) {
          //separatedData(hour): The row data excluding the first 24 elements (which are the one-hot encoded
          // hour indicators) is prepended to the list corresponding to the current hour.
          separatedData(hour) = array.slice(24, array.length) :: separatedData(hour)
          //The programmedPow value corresponding to the current row's index is prepended to the list for the current hour.
          separatedPotProgramada(hour) = programmedPow(index) :: separatedPotProgramada(hour)
          //the realPow value is prepended to the list for the current hour.
          separatedPotReal(hour) = realPow(index) :: separatedPotReal(hour)
        }
      }
    }

    val nInputs: Int = separatedData(0).headOption.map(_.size).getOrElse(0)
    val nHidden: Int = (1.9 * nInputs).toInt
    val arrayWeights: Array[Array[Double]] = Array.fill(24)(Array.empty[Double])
    val nWeights: Int = nHidden * (nInputs + 1)
    val n = nWeights
    //////////
    val start = System.nanoTime()
    // Initialize an array to store the best fitness value at each iteration
    val convergenceCurve: Array[Double] = Array.fill(I)(Double.MaxValue)
    //Execution of the DSPSO (Distributed Synchronous PSO) variant of the PSO algorithm
    for (hour <- 0 until 24) {
      particles = Array.empty[Array[Double]]
      best_global_position = Array.empty[Double]
      best_global_fitness = Double.MaxValue
      // Convert the lists into serializable arrays
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
      //Process: create RDD
      var rdd_master = sc.parallelize(particles)
      //Optimization loop
      for (i <- 0 until I) {
        val local_accum_pos: CollectionAccumulator[Array[Double]] = sc.collectionAccumulator[Array[Double]]("BestLocalPositions")
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
        //Store the best fitness for this iteration
        //keep track of the best fitness value found at each iteration
        convergenceCurve(i) = best_global_fitness
      }
      arrayWeights(hour) = best_global_position
    }
    //Plotting the convergence curve allows you to analyze the convergence behavior
    // of the optimization algorithm and assess whether it is effectively improving over time.
    //println(s"Convergence curve: ${convergenceCurve.mkString(",")}") for debugging
    //Save the best global fitness values to a file for later analysis
    val symbols = new DecimalFormatSymbols(Locale.US)
    val formatterDecimal = new DecimalFormat("#0.0000000000000000", symbols)  // 16 decimal places

    val writer_conv = new PrintWriter(new java.io.File("convergence_curve.csv"),"UTF-8")
    try {
      convergenceCurve.foreach { value =>
        val formattedValue = formatterDecimal.format(value)
        //println(s"Formatted for file: $formattedValue") // Check formatted value in console
        writer_conv.println(formattedValue)
      }
    } finally {
        writer_conv.flush()
        writer_conv.close()  // Ensure the writer is closed
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
    //Prediction
    for (hour <- 0 until 24) {
      for (i <- 0 until separatedData(hour).length) {
        val pot = forwardProp(separatedData(hour)(i), arrayWeights(hour), nInputs, nHidden)
                predictedPower(hour) = predictedPower(hour) :+ pot
      }
    }
    val writer= new PrintWriter(new java.io.File("output.csv"),"UTF-8")
    val currentDateTime = LocalDateTime.now()
    val formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")
    val formattedDateTime = currentDateTime.format(formatter)
    writer.println(formattedDateTime)
    try{
      for (hour <- 0 until 24) {
        writer.println(keyValueMap(hour))
        nph=separatedData(hour).length
        println(s"Number of predictions/per hour:$hour: %d".format(nph))
        for ((real, predicted) <- separatedPotReal(hour).zip(predictedPower(hour))) {
          writer.println(f"$real%.16f $predicted%.16f")
        }
      }
    }
    finally {writer.close()}
    ////Time measurement
    val time = (end - start) / 1e9
    println(s"Execution time(s):$time")
  }
  private var nph = 0
}

