import java.io.{FileWriter, PrintWriter}
import java.text.{DecimalFormat, DecimalFormatSymbols}
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import java.util.Locale
import scala.io.Source
/**
 * @author Manuel I. Capel
 */
object Programa {
  val FA = new Auxiliary_Functions
  def main(args: Array[String]): Unit = {
   // val fileName = "demanda_limpia_2020.csv"//size : 175104 data
   val fileName = "demanda_limpia_final.csv"
  //val numRowsToKeep = List(1008, 12000, 30240)// Number of data to process from the dataset
  // val numRowsToKeep = List(1200)
 val numRowsToKeep = List(30240)
 //val numRowsToKeep = List(175104)
 //Files for the graphic plots
  val file_1 = new FileWriter(s"errors_iter_part.txt", true)
  val file_2 = new FileWriter(s"time_iters.txt", true)
  val graf_1 = new PrintWriter(file_1)
  val graf_2 = new PrintWriter(file_2)
  for (nr<-numRowsToKeep) {
    val pos_max: Double = 0.8
    var dataRows = Source.fromFile(fileName).getLines.drop(1).filter { line =>
      val cols = line.split(",").map(_.trim)
      cols.forall(_.nonEmpty) //for empty values filtering of rows
    }.take(nr).toList.map { line =>
      val cols = line.split(",").map(_.trim)
      cols
    }
    //The result is stored in dataRows, which is a List[Array[String]]. Each element of
    //this list corresponds to a row in the CSV, and each row is represented as an array
    //of strings (one for each column).
  val dates = dataRows.map(_(4))
  val realPower = dataRows.map(_(1)).map(_.toDouble)
  val programmedPower = dataRows.map(_(3)).map(_.toDouble)
  val (days, hours) = FA.separateDayHourMinuteSecond(dates)
  val daysOfWeek = FA.convertToDayOfWeek(days)
  var (h, mi) = FA.separateHourMinute(hours)
  val oneHotHours = FA.encode(h)
  val oneHotMinutes = FA.encode(mi)
  val oneHotDays = FA.encode(daysOfWeek)
  val combinedMatrix1 = oneHotHours.zip(oneHotDays).map { case (rowA, rowB) => rowA ++ rowB }
  val combinedMatrix2 = combinedMatrix1.zip(oneHotMinutes).map { case (rowA, rowB) => rowA ++ rowB }
  val dataList = combinedMatrix2.zip(programmedPower).map { case (row, value) => row :+ value }
  //The final output data is a List[Array[Double]], which can be fed into a machine learning model
  // for training or testing.
  val data: List[Array[Double]] = dataList.map(_.toArray)
  //The following code snippet effectively organizes data by hour based on
  // one-hot encoded hour information, allowing you to analyze or process data specific
  // to each hour of the day.
  val separatedData: Array[List[Array[Double]]] = Array.fill(24)(List.empty)
  val separatedRealPower: Array[List[Double]] = Array.fill(24)(List.empty)
  val separatedProgrammedPower: Array[List[Double]] = Array.fill(24)(List.empty)
  for ((array, index) <- data.zipWithIndex) {
    for (hour <- 0 until 24) {
      if (array(hour) == 1.0) {
        //separatedData(hour): The row data excluding the first 24 elements (which are the one-hot encoded
        // hour indicators) is prepended to the list corresponding to the current hour.
        separatedData(hour) = array.slice(24, array.length) :: separatedData(hour)
        separatedRealPower(hour) = realPower(index) :: separatedRealPower(hour)
        separatedProgrammedPower(hour) = programmedPower(index) :: separatedProgrammedPower(hour)
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
  val num_iterations = List(100)
  // num_iterations = List(100, 500, 1000)
  val num_particles = List(100)
  //val num_particles = List(100,500,1000)
  // Initialize an array to store the best fitness value at each iteration
  val convergenceCurve: Array[Double] = Array.fill(num_iterations(0))(Double.MaxValue)
    for (iters <- num_iterations) {
      for (parts <- num_particles) {
        // Number of particles
        val m = parts
        // Number of iterations
        val I = iters

        val existingFile = new FileWriter(s"results_ANN_energies_Full$I _$m _$nr.txt", true)
        val weightsFile = new FileWriter(s"weights_vector_ANN_energies$I _$m _$nr.csv", true)
        val outputFile = new PrintWriter(existingFile)
        val weigthOutput = new PrintWriter(weightsFile)
        ////
        val start = System.nanoTime()

        //Execution of the DAPSO variant of the algorithm
        for (hour <- 0 until 24) {
          val trainer = new DAPSO(separatedData(hour), separatedProgrammedPower(hour), nInputs, nHidden, I, m, pos_max, convergenceCurve)
          trainer.init_weights()
          trainer.processing()
          arrayWeights(hour) = trainer.get_weights()
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
        val end = System.nanoTime()
        ////////////////The calculated weights are stored in a file
        for (hour <- 0 until 24) {
          val best_glob_position = arrayWeights(hour)
          weigthOutput.println(best_glob_position.mkString(", "))
        }
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
        ///////////////
        var predictedPower: Array[List[Double]] = Array.fill(24)(List.empty[Double])
        //Prediction
        for (hour <- 0 until 24) {
          for (i <- 0 until separatedData(hour).length) {
            val pot = FA.forwardProp(separatedData(hour)(i), arrayWeights(hour), nInputs, nHidden)
                        predictedPower(hour) = predictedPower(hour) :+ pot
          }
        }
        //Results
        val writer= new PrintWriter(new java.io.File("output.csv"),"UTF-8")
        val currentDateTime = LocalDateTime.now()
        val formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")
        val formattedDateTime = currentDateTime.format(formatter)
        writer.println(formattedDateTime)
        for (hour <- 0 until 24) {
          //println("Results for " + keyValueMap(hour))
          outputFile.println("Results for " + keyValueMap(hour))
          //writer.write("results for " + keyValueMap(hour))
          //writer.write("\n")
          //writer.write("Weights for " + keyValueMap(hour) + ": " + arrayWeights(hour).mkString(", "))
          //writer.write("\n")
          writer.println(keyValueMap(hour))
          nph=separatedData(hour).length
          println(s"Number of predictions/per hour:$hour: %d".format(nph))
          for ((real, predicted) <- separatedRealPower(hour).zip(predictedPower(hour))) {
           //println(s"Real power: $real - Predicted power: $predicted")
           outputFile.println(f"$real%.16f,$predicted%.16f")
           // writer.write(s"Electric power real: $real - Electric power predicted: $predicted")
            writer.println(f"$real%.16f $predicted%.16f")
          }
        }
        writer.close()
        weigthOutput.close()
        ////
        val time = (end - start) / 1e9
        println(s"Time of execution(s):$time")
        outputFile.println(s"$time")
        outputFile.close()
        val error = FA.MSEOfDataSeparated(separatedData, separatedRealPower, arrayWeights, nInputs, nHidden)
        //Filling the files for the graphic plots
        var error_ = error / 24.0
        graf_1.println(s"$iters, $parts, $numRowsToKeep, $error_")
        graf_2.println(s"$iters, $parts, $numRowsToKeep, $time")
      }
    }
  }
  graf_1.close()
  graf_2.close()

  }
  private var nph = 0
}
