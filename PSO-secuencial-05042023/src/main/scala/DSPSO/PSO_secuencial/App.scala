package DSPSO.PSO_secuencial
  

object App {
  import scala.util.Random
  import scala.math.sqrt

  def MSE(y: Array[Double], pred: Array[Double]): Double = {
    val n = y.length
    if (n != pred.length) {
      println("error: datos y predicción de distintos tamaños")
      return -1
    }
    var resultado = 0.0
    for (i <- 0 until n) {
      resultado += math.pow(y(i) - pred(i), 2)
    }
    resultado /= n
    resultado
  }
  
  //def foo(x : Array[String]) = x.foldLeft("")((a,b) => a + b)
  def main(args : Array[String]):Unit= {
    println( "Hello World!" )
    //println("concat arguments = " + foo(args))
    var tiempo_fitness = 0.0
    val V_max = 10
    val W = 1.0
    val c_1 = 0.3
    val c_2 = 0.7
    val objetivo = Array[Double](50, 50, 50)
    val n = 100000 // número de iteraciones
    val m = 10 // número de partículas
    var posiciones = Array[Array[Double]]()
    var velocidades = Array[Array[Double]]()
    var mejores_pos_locales = Array[Array[Double]]()
    var best_local_fitness = Array[Double]()
    var best_global_fitness: Double = 0.0
    
    // Inicializacion
    for (i <- 0 until m) {
      val aux = Array.fill(3)(Random.nextDouble() * 200 - 100)
      posiciones = posiciones :+ aux
      velocidades = velocidades :+ Array.fill(3)(Random.nextDouble() * 200 - 100)
      mejores_pos_locales = mejores_pos_locales :+ aux
    }
    var mejor_pos_global = posiciones(0)
    val start = System.nanoTime()

    // Bucle principal
    for (i <- 0 until n) {
     for (j <- 0 until m) {
        val start_fitness = System.nanoTime()
        val fit = MSE(posiciones(j), objetivo)
        val end_fitness = System.nanoTime()
        tiempo_fitness += (end_fitness - start_fitness)
        if (i == 0) {
          best_local_fitness = best_local_fitness :+ fit
          if (j == 0) {
            best_global_fitness = fit
          } else if (fit < best_global_fitness) {
            best_global_fitness = fit
            mejor_pos_global = posiciones(j).clone()
          }
        } else {
          if (fit < best_local_fitness(j)) {
            best_local_fitness = best_local_fitness.updated(j, fit)
            mejores_pos_locales = mejores_pos_locales.updated(j, posiciones(j).clone())
            if (fit < best_global_fitness) {
              best_global_fitness = fit
              mejor_pos_global = posiciones(j).clone()
            }
          }
        } 
     }//for(j<- ...)
     //Actualizacion de las posiciones, velocidades
     val r_1 = Random.nextDouble()
     val r_2 = Random.nextDouble()
     for (j <- 0 until m) {
        for (k <- 0 until 3) {
          velocidades = velocidades.updated(j, velocidades(j).updated(k, W * velocidades(j)(k) + c_1 * r_1 * (mejores_pos_locales(j)(k) - posiciones(j)(k)) + c_2 * r_2 * (mejor_pos_global(k) - posiciones(j)(k))))
          if (velocidades(j)(k) > V_max) {
         //   velocidades = velocidades.updated(j, velocidades(j).updated(k, V_max))
             velocidades(j)(k) = V_max
          } else if (velocidades(j)(k) < -V_max) {
  //          velocidades = velocidades.updated(j, velocidades(j).updated(k, -V_max))
              velocidades(j)(k) = -V_max
          }

          posiciones = posiciones.updated(j, posiciones(j).updated(k, posiciones(j)(k) + velocidades(j)(k)))
        }//for(k<- ...)
      }//for(j<- ...)
    }//Bucle principal
     val end = System.nanoTime()
     val tiempo = (end - start) / 1e9
     println("Tiempo de ejecucion(s): " + tiempo)
     println("Tiempo de ejecucion fitness(s): " + tiempo_fitness / 1e9)
     println("mejor posición encontrada: " + mejor_pos_global.mkString("[", ", ", "]"))
  }//main()
}