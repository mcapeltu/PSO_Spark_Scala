import scala.util.Random

class pso_secuencial(x: List[Array[Double]], y: List[Double], nInputs: Int, nHidden: Int, nIters: Int, nParts: Int, pos_max: Double) {
  val FA = new Funciones_Auxiliares
  val nWeights: Int =nHidden*(nInputs + 1)
  // Número dimensiones de los pesos
  val n = nWeights
  // Número de partículas
  val m = nParts
  // Número de iteraciones
  val I = nIters
  var particulas = Array.empty[Array[Double]]
  var mejor_pos_global = Array.empty[Double]
  var best_global_fitness = Double.MaxValue
  val rand = new Random
  val W = 1.0
  val c_1 = 0.8
  val c_2 = 0.2
  val V_max = 0.6*pos_max
  // Convertir las listas a arrays serializables
  val xSer: Array[Array[Double]] = x.toArray
  val ySer: Array[Double] = y.toArray

  // Inicializamos los vectores
  def inicializar_pesos(){
      for (i <- 0 until m) {
        //Aquí puede que sea mejor inicializar las últimas posiciones a valores más pequeños
        val posicion = Array.fill(nWeights)(FA.Uniform(2, rand))
        val velocidad = Array.fill(nWeights)(FA.Uniform(2, rand))
        val fit = FA.MSERed(xSer, ySer, posicion, nInputs, nHidden)
        val part_ = posicion ++ velocidad ++ posicion ++ Array(fit)
        if (fit < best_global_fitness) {
          best_global_fitness = fit
          mejor_pos_global = posicion
        }
        particulas = particulas :+ part_
      }
    //(best_global_fitness, mejor_pos_global, parts_)
  }

  def procesar(): Unit = {
    for (i <- 0 until I) {
      println(i + " iteraciones")
      for (j <- 0 until m) {
        var particula = particulas(j)
        particula = FA.fitnessEval(xSer, ySer, particula, nInputs, nHidden)
        val fit = particula(3*nWeights)
        if (fit < best_global_fitness) {
          best_global_fitness = fit
          mejor_pos_global = particula.slice(0, nWeights)
        }
        particula = FA.posEval(particula, mejor_pos_global, nWeights, rand, W, c_1, c_2, V_max, pos_max)
        particulas(j) = particula
      }
    }
  }

  def get_pesos(): Array[Double] = {
    mejor_pos_global
  }
}
