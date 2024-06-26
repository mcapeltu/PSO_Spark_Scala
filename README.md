--------------------------------
1. DSPSO
--------------------------------
This is a repository to upload parallel implementations of the PSO algorithm in Spark. The
characteristics of the graphics card I used for the 2 distributed PSO implementations are detailed below. As sample space we used the reduced version "demanda_limpia_2020" which only contains the 2020 data from the dataset used in the paper "A Distributed Particle Swarm Optimization Algorithm Based on Apache Spark for Asynchronous Parallel Training of Deep Neural Networks" (2024). The reason for this is that we do not have the full authorship of the cleanup of the original dataset. Therefore, the program results may not be as accurate as those obtained for the paper, obtained with the full data set.

device = <CUDA device 0 'b'NVIDIA GeForce RTX 3050 Ti Laptop GPU''>
GPU compute capability:  (8, 6)
GPU total number of SMs:  20
total cores:  2560
cores/SM=  128

-----------------------------
Results of a sample run of the program DSPSO (written in output file):
2024-03-28 12:41:14
results for 00:00 hours
Weights for 00:00: -13,450769716034100	-47,728293846234500	59,266077375705600	49,965774801865000	-72,966566423305300	-21,982980721547300	77,601918627710800
 ... (390 values)

Electric power real: 27.042 - Electric power predicted: 26.248732053683483   ... (53 values: 1 prediction/week day and hour)

Results of a sample run of the program DSPSO:

Execution time(s):7.268051  

Best global fitness:4.123869710203265

Best global position:43.1731774347513, 21.473066093976275, 31.0475325756545, 3.276828495610026, -50.37918609715331, -8.532274854345587, -50.53698759479768, 39.139713249493326, 7.417429706143494, 9.667756224396852, -39.719196020735076, -38.25570193227723, 96.85690616248132, 39.78847568780321, -28.549565328521197, -21.03202915635695, 20.476994352601576, 33.83330282561311, 30.60461510223907, -43.815938925645014, -36.944106535066894, 33.06628696032726, -35.406399130295085, 63.2501052668746, -35.278956631943814, -7.8340691145532855, 47.55186547943748, -39.41109527664804, -75.22326896757431, 6.59895823921272, -41.15025179175568, -29.91737417495375, 8.245179353059765, 23.171624889009983, 14.265484460521861, -46.918242600801904, 11.733272891099633, -11.554286694478634, -54.190827425852184, 65.11055899625534, -0.15896279034309657, 14.965584781876714, -80.91723583942851, 47.36898783767546, -43.36502943759824, -39.24761420791072, 29.454851319771308, -1.5526891887419083, 53.01738915386933, 72.32378588587467, -34.22573860765243, 63.07223640198091, -36.04560136969562, -22.344255419901394, 23.600526444419096, -50.964973102996396, 37.51231601864893, 15.957479025685817, 9.381360085252268, 100.7104292452104, -59.577889937918, 58.01328172155618, 31.938668615126726, -41.51287969620454, -5.184354192186021, -6.3200123187057695, -64.36174826298479, 44.312142480888575, 30.00266108945757, 18.142902349749647, -46.007240769146506, 40.89745202356365, 4.974935122075806, -69.45726571709996, -36.27700749443735, -52.774997071529455, 16.379878143703504, -21.172684997430217, 58.05683236910996, 61.62417451130438, -6.200613170994105, 24.54504164021948, 58.70524587815213, 2.623793584122537, 5.441427636435458, -44.25912994677219, 7.903038078252789, -60.22037760557592, 41.00641390962291, 18.312074908311452, -24.66110042275511, ... (390 values)

And the obtained weights of the 24 ANN (for 0-23 hours):
-7.938420307667414, -15.12131847149081, 50.81155622887553, -78.15175172391312, 36.01077741831762, -3.7054185663393744, 17.554487918394493, -27.692365791725155, 6.929870746199421, -13.034891909653293, -27.89013598420111, 39.64969279559443, -42.991555387046596, 65.4745477838138, 33.5442342066032, -47.058966289853856, -54.08574012027199, 88.81480527232387, 64.46853389182638, 39.58829327087747, -26.264689644089614, -34.19850914935054, 42.10223548912699, -13.73631971768673, 18.031604746793022, 67.67961557340664, ....

--------------------------------
2. DAPSO
--------------------------------
The compressed file 'ANN_Energies4hour_binary-one-hot-encoding.7z' is the distributed asynchronous implementation of the PSO explained in the paper:  "GPU-Accelerated PSO for Neural Network-Based Energy Consumption Prediction" (2024).

-------------------------

Apache Spark configuration:
export SPARK_HOME= /opt/spark-3.5.0-bin-hadoop3.x-Scala2.13

Spark cluster setting:

To launch ![cluster-screen](https://github.com/mcapeltu/PSO_Spark_Scala/assets/12482867/ab3a9050-f4e4-4abd-ac51-fa56e37a303a)


the Master node: sudo $SPARK_HOME/bin/start-master.sh --host localhost

To launch each of the worker nodes: sudo $SPARK_HOME/bin/start-worker.sh localhost:7077

-------------------------------------------
The compiled application ANN_Energies4hour_binary-one-hot-encoding has to be sent in "jar" format to the Spark cluster-nodes:

a) First, generate the jar; go to the subdirectory of the Spark project and type 
>sbt.clean

>sbt.assembly

b) The .jar file will be generated in the subdirectory 'target/Scala-2.12' of the IntelliJ project. To get it functioning, in the 'package.scala' file you have to include:
SparkSession.builder().config(...).master("spark:localhost:7077").getOrCreate().
And in the application: val spark= createSparkSession("name",isLocal=false)

3. Results
---------------------------------------
DSPSO:

![Recurso 1](https://github.com/mcapeltu/PSO_Spark_Scala/assets/12482867/4cebf9bc-e315-4698-87ef-c8ce9085f782)

DAPSO:



-------![Recurso 1](https://github.com/mcapeltu/PSO_Spark_Scala/assets/12482867/fb6883d4-28cb-4008-9521-0c4b4dbfcef5)
------------------
 JVM provided by Java JDK 1.8.0_211 has been used.

---------------------



