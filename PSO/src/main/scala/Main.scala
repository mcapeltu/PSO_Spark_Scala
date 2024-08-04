import org.apache.spark.{SparkConf, SparkContext}

import java.io.{FileWriter, PrintWriter}
import java.text.{DecimalFormat, DecimalFormatSymbols}
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import java.util.Locale
import scala.io.Source
import scala.util.Random
object Main {
  val conf = new SparkConf()
    .setAppName("Sequential PSO algorithm ")
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
  ////
  val FA = new AuxiliaryFunctions
  // Number of particles
  val m = 100
  // Number of iterations
  val I = 100
  var particles = Array.empty[Array[Double]]
  var best_global_position = Array.empty[Double]
  var best_global_fitness = Double.MaxValue
  val pos_max: Double = 0.8
  ////random generation
  val rand = new Random
  ///PSO input parameters
  val W = 1.0  // Inertia weight controlling the impact of the previous velocity
  val c_1 = 0.8 // Cognitive coefficient controlling the attraction to the particle's own best position
  val c_2 = 0.2 // Social coefficient controlling the attraction to the global best position.
  val V_max = 10.0 // Maximum allowable velocity for the particles

  def main(args: Array[String]): Unit = {
    //println("Hello world!")
    val fileName = "demanda_limpia_final.csv"
    val numRowsToKeep: Int =30240
    // dataRows is a List[Array[String]]. Each element of this list corresponds to a row in
    // the CSV file, and each row is represented as an array of strings (one for each column).
    var dataRows = Source.fromFile(fileName).getLines.drop(1).filter { line =>
      val cols = line.split(",").map(_.trim)
      cols.forall(_.nonEmpty) // Filter rows with non-empty values
    }.take(numRowsToKeep).toList.map { line =>
      val cols = line.split(",").map(_.trim)
      cols
    }
    /// Data preprocessing
    val dates = dataRows.map(_(4))
    val realPow = dataRows.map(_(1)).map(_.toDouble)
    val programmedPow = dataRows.map(_(3)).map(_.toDouble)
    val (days, hours) = FA.separateDayHourMinuteSecond(dates)
    val daysOfWeek = FA.convertToDayOfWeek(days)
    var (h, mi) = FA.separateHourMinute(hours)
    val oneHotHours = FA.encode(h)
    val oneHotMinutes = FA.encode(mi)
    val oneHotDays = FA.encode(daysOfWeek)
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
    val separatedRealPow: Array[List[Double]] = Array.fill(24)(List.empty)
    val separatedProgrammedPow: Array[List[Double]] = Array.fill(24)(List.empty)
    for ((array, index) <- data.zipWithIndex) {
      for (hour <- 0 until 24) {
        if (array(hour) == 1.0) {
          //separatedData(hour): The row data excluding the first 24 elements (which are the one-hot encoded
          // hour indicators) is prepended to the list corresponding to the current hour.
          separatedData(hour) = array.slice(24, array.length) :: separatedData(hour)
          //The programmedPow value corresponding to the current row's index is prepended to the list for the current hour.
          separatedProgrammedPow(hour) = programmedPow(index) :: separatedProgrammedPow(hour)
          //the realPow value is prepended to the list for the current hour.
          separatedRealPow(hour) = realPow(index) :: separatedRealPow(hour)
        }
      }
    }
    val convergenceCurve: Array[Double] = Array.fill(I)(Double.MaxValue)
    val nInputs: Int = separatedData(0).headOption.map(_.size).getOrElse(0)
    val nHidden: Int = (1.9 * nInputs).toInt
    val arrayWeights: Array[Array[Double]] = Array.fill(24)(Array.empty[Double])
    val nWeights: Int = nHidden * (nInputs + 1)
    ////////////////
    val start = System.nanoTime()
    ////Execution of PSO
    for (hour <-0 until 24){
      val PSO= new sequentialPSO(separatedData(hour),separatedRealPow(hour), nInputs, nHidden, I, m, pos_max, convergenceCurve)
      PSO.init_weights()
      PSO.processing()
      arrayWeights(hour)= PSO.get_weights()
    }
    //Plotting the convergence curve allows you to analyze the convergence behavior
    // of the optimization algorithm and assess whether it is effectively improving over time.
    //println(s"Convergence curve: ${convergenceCurve.mkString(",")}") for debugging
    //Save the best global fitness values to a file for later analysis
    val symbols = new DecimalFormatSymbols(Locale.US)
    val formatterDecimal = new DecimalFormat("#0.0000000000000000", symbols)  // 16 decimal places
    //println(s"Convergence curve: ${convergenceCurve.mkString(",")}")
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
    ///End of measured time: we are measuring only training
    val end = System.nanoTime()
    ///Preparing output
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
    ////Performing predictions by hour
    var predictedPower: Array[List[Double]] = Array.fill(24)(List.empty[Double])
    //Prediction
    for (hour <- 0 until 24) {
      for (i <- 0 until separatedData(hour).length) {
        val pot = FA.forwardProp(separatedData(hour)(i), arrayWeights(hour), nInputs, nHidden)
        predictedPower(hour) = predictedPower(hour) :+ pot
      }
    }
    ///Results
    val writer= new PrintWriter(new java.io.File("output.csv"),"UTF-8")
    val currentDateTime = LocalDateTime.now()
    val formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")
    val formattedDateTime = currentDateTime.format(formatter)
    writer.println(formattedDateTime)
    for(hour<-0 until 24){
      writer.println(keyValueMap(hour))
      nph=separatedData(hour).length
      //println(s"Number of predictions/per hour:$hour: %d".format(nph))
      for ((real, predicted) <- separatedRealPow(hour).zip(predictedPower(hour))){
        writer.println(f"$real%.16f $predicted%.16f")
      }
    }
    writer.close()
    ////Time spent
    val time = (end - start) / 1e9
    ////Error
    val error = FA.MSEOfDataSeparated(separatedData, separatedRealPow, arrayWeights, nInputs, nHidden)
    println(s"Time of execution(s):$time, MSE of predicted power:$error")
  }
  private var nph = 0
}