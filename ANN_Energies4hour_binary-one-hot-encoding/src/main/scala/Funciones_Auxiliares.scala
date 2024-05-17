import org.apache.spark.util.CollectionAccumulator

import java.time.LocalDate
import java.time.format.DateTimeFormatter
import scala.collection.mutable.ListBuffer
import scala.io.Source
import scala.math.signum
import scala.util.Random
class Funciones_Auxiliares {
  // Calcula wT*x(i) (Valor predicho para el dato x(i))
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
    //Paso de capa inicial a oculta
    for (j <- 0 until nHidden) {
      //println("longitud weights: " + weights.length)
      val weights1 = weights.slice(nInputs * j, nInputs * (j + 1))
      var resultado = 0.0
      for (k <- 0 until nInputs) {
        //println("nInputs: " + nInputs)
        resultado += x(k) * weights1(k)
      }
      z2(j) = resultado
    }
    //paso de la capa de neuronas ocultas a la de salida
    var z3 = 0.0
    val weights2 = weights.slice(nInputs * nHidden, weights.length)
    for (k <- 0 until nHidden) {
      z3 += z2(k) * weights2(k)
    }
    z3 //no se aplica la tanh en la capa de salida
  }

  def MSERed(x: Array[Array[Double]], y: Array[Double], weights: Array[Double], nInputs: Int, nHidden: Int): Double = {
    var resultado = 0.0
    val n_datos = x.length

    for (i <- 0 until n_datos) {
      //println("nInputs: " + nInputs)
      //println("nHidden: " + nHidden)
      val pred = forwardProp(x(i), weights, nInputs, nHidden)
      //println("pred: " + pred)
      resultado += math.pow(y(i) - pred, 2)
    }
    resultado /= n_datos
    resultado
  }

  def fitnessEval(x: Array[Array[Double]], y: Array[Double], particula_pesos: Array[Double], nInputs: Int, nHidden: Int): Array[Double] = {
    val nWeights: Int = nHidden * (nInputs + 1)
    if (particula_pesos == null) {
      println("El array de pesos es null")
      return Array.empty[Double]
    }
    val best_fit_local = particula_pesos(3 * nWeights)
    val weights = particula_pesos.slice(0, nWeights)
    val fit = MSERed(x, y, weights, nInputs, nHidden)
    if (fit < best_fit_local) {
      particula_pesos(3 * nWeights) = fit
      for (k <- 0 until nWeights) {
        particula_pesos(2 * nWeights + k) = weights(k)
      }
    }
    particula_pesos
  }

  def posEval(part: Array[Double], mpg: Array[Double], N: Int, rand: Random, W: Double, c_1: Double, c_2: Double, V_max: Double, pos_max: Double): Array[Double] = {
    val velocidades = part.slice(N, 2 * N)
    val mpl = part.slice(2 * N, 3 * N)
    val r_1 = rand.nextDouble()
    val r_2 = rand.nextDouble()
    for (k <- 0 until N) {
      velocidades(k) = W * velocidades(k) + c_1 * r_1 * (mpl(k) - part(k)) + c_2 * r_2 * (mpl(k) - part(k))
      if (velocidades(k) > V_max) {
        velocidades(k) = V_max
      } else if (velocidades(k) < -V_max) {
        velocidades(k) = -V_max
      }
      part(k) = part(k) + velocidades(k)
      if (part(k) > pos_max) {
        part(k) = pos_max
      } else if (part(k) < -pos_max) {
        part(k) = -pos_max
      }
      part(N + k) = velocidades(k)
    }
    part
  }

  def modifyAccum(part: Array[Double], N: Int, local_accum_pos: CollectionAccumulator[Array[Double]], local_accum_fit: CollectionAccumulator[Double]): Unit = {
    local_accum_pos.add((part.slice(2 * N, 3 * N)))
    local_accum_fit.add(part(3 * N))
  }

  // Convertir el Array[Array[Double]] a un ListBuffer[Array[Double]]
  def toListBuffer(arrayArray: Array[Array[Double]]): ListBuffer[Array[Double]] = {
    val listBuffer: ListBuffer[Array[Double]] = ListBuffer.empty[Array[Double]]

    // Recorrer cada subarray en el Array[Array[Double]] y convertirlo a un List[Double]
    for (subArray <- arrayArray) {
      val list: List[Double] = subArray.toList
      // Agregar el List[Double] al ListBuffer[Array[Double]]
      listBuffer += list.toArray
    }
    listBuffer
  }

  //Operaciones que tienen que ver con el formato de datos de consumo eneria/tiempo
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

  //Genera un número aleatoriamente entre -a y a
  def Uniform(a: Double, rand: Random): Double = {
    val num = rand.nextDouble() * 2 * a // genera un número aleatorio entre 0.0 y 2a
    val ret = num - a
    ret
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

  def MSEOfDataSeparated(dataSeparated: Array[List[Array[Double]]], valorSeparated: Array[List[Double]], arrayWeights: Array[Array[Double]], nInputs: Int, nHidden: Int): Double = {
    var resultado = 0.0
    for (hour <- 0 until 24){
      val x = dataSeparated(hour).toArray
      val y = valorSeparated(hour).toArray
      val aux: Double = MSERed(x, y, arrayWeights(hour), nInputs, nHidden)
      resultado += aux
    }
    resultado = resultado / 24.0
    resultado
  }

  def MSEOfData(pred: List[Double], valor: List[Double]): Double = {
    var resultado = 0.0
    for (i <- 0 until pred.length)
    resultado += math.pow(valor(i) - pred(i), 2)

    resultado = resultado / pred.length
    resultado
  }
}
