# PSO_Spark_Scala
--------------------------------
This is a repository to upload parallel implementations of the PSO algorithm in Spark. The
characteristics of the board I used for the 2 distributed PSO implementations are detailed below. As sample space we used the reduced version "demand_limpia_2020" which only contains the 2020 data from the dataset used in the paper "GPU-Accelerated PSO for Neural Network-Based Energy Consumption Prediction" (2024). The reason for this is that we do not have the full authorship of the cleanup of the original dataset. Therefore, the program results may not be as accurate as those obtained for the paper, obtained with the full data set.
device = <CUDA device 0 'b'NVIDIA GeForce RTX 3050 Ti Laptop GPU''>
GPU compute capability:  (8, 6)
GPU total number of SMs:  20
total cores:  2560
cores/SM=  128
-----------------------------
Results of a sample run of the program DSPSO (written in output file):
2024-03-28 12:41:14
results for 00:00
Weights for 00:00: -13,450769716034100	-47,728293846234500	59,266077375705600	49,965774801865000	-72,966566423305300	-21,982980721547300	77,601918627710800
 ... (390 values)
Electric prower real: 27.042 - Electric power predicted: 26.248732053683483   ... (53 values)
Results of a sample run of the program DSPSO:
Execution time(s):7.268051
And the obtained weigths of the 24 ANN (for 0-23 hours):
-7.938420307667414, -15.12131847149081, 50.81155622887553, -78.15175172391312, 36.01077741831762, -3.7054185663393744, 17.554487918394493, -27.692365791725155, 6.929870746199421, -13.034891909653293, -27.89013598420111, 39.64969279559443, -42.991555387046596, 65.4745477838138, 33.5442342066032, -47.058966289853856, -54.08574012027199, 88.81480527232387, 64.46853389182638, 39.58829327087747, -26.264689644089614, -34.19850914935054, 42.10223548912699, -13.73631971768673, 18.031604746793022, 67.67961557340664, ....
-------------------------
 JVM provided by Java JDK 1.8.0_211 has been used.
---------------------



