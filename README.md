# PSO_Spark_Scala
--------------------------------
Put the folder DU in the Eclipse workspace, import the project in the workspace into Scala IDE and "run as" as Scala application.
-----------------------------------
This a repository to upload parallel implementations of PSO algorithm on Spark
Characteristics of the card I used for the DU synchronous implementation of PSO. The sample used the target(50,50,50) and 10000 iterations:
device = <CUDA device 0 'b'NVIDIA GeForce RTX 3050 Ti Laptop GPU''>
GPU compute capability:  (8, 6)
GPU total number of SMs:  20
total cores:  2560
cores/SM=  128
-------------------------
Results of a sample run of the program DU:
Tiempo de ejecucion(s): 200.17689
Tiempo de ejecucion fitness(s): 4.5307825000000195
Tiempo de ejecucion poseval(s): 5.211436600000015
Tiempo de ejecucion global fitness(s): 0.03162360000000045
Tiempo de ejecucion collect(s): 92.97524110000009
Tiempo de ejecucion foreach(s): 96.1983850000001
mejor_pos_global-> [49.9999019754346, 50.00253922658899, 50.000225150569285]
mejor fitness global-> 2.169324421507735E-6, 2.169324421507735E-6
--------------------------
Eclipse Scala IDE Build id: 4.7.0-vfinal-2017-09-29T14:34:02Z-Typesafe with the JVM provided by Java JDK 1.8.0_211 has been used.
---------------------
It is necessary to use an archetype that gives Maven-Scala features, such as the one in: http://alchim31.free.fr/m2e-scala/update-site/
-----------------------
The POM.xml for Maven libraries to work:
<project xmlns="http://maven.apache.org/POM/4.0.0" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:schemaLocation="http://maven.apache.org/POM/4.0.0 http://maven.apache.org/maven-v4_0_0.xsd">
  <modelVersion>4.0.0</modelVersion>
  <groupId>DSPSO</groupId>
  <artifactId>DU</artifactId>
  <version>0.0.1-SNAPSHOT</version>
  <name>${project.artifactId}</name>
  <description>My wonderfull scala app</description>
  <inceptionYear>2018</inceptionYear>
  <licenses>
    <license>
      <name>My License</name>
      <url>http://....</url>
      <distribution>repo</distribution>
    </license>
  </licenses>

  <properties>
    <maven.compiler.source>1.8</maven.compiler.source>
    <maven.compiler.target>1.8</maven.compiler.target>
    <encoding>UTF-8</encoding>
    <scala.version>2.12.6</scala.version>
    <scala.compat.version>2.12</scala.compat.version>
    <spec2.version>4.2.0</spec2.version>
    <spark.version>3.3.1</spark.version>
  </properties>

  <dependencies>
    <dependency>
      <groupId>org.scala-lang</groupId>
      <artifactId>scala-library</artifactId>
      <version>${scala.version}</version>
    </dependency>
<!-- Spark -->
	<dependency>
		<groupId>org.apache.spark</groupId>
		<artifactId>spark-core_2.12</artifactId>
		<version>${spark.version}</version>
	</dependency>
	<dependency>
		<groupId>org.apache.spark</groupId>
		<artifactId>spark-sql_2.12</artifactId>
		<version>${spark.version}</version>
	</dependency>
	<dependency>
		<groupId>org.apache.spark</groupId>
		<artifactId>spark-mllib_2.12</artifactId>
		<version>${spark.version}</version>
	</dependency>
	<dependency>
		<groupId>org.apache.spark</groupId>
		<artifactId>spark-graphx_2.12</artifactId>
		<version>${spark.version}</version>
	</dependency>
	<dependency>
		<groupId>org.apache.spark</groupId>
		<artifactId>spark-yarn_2.12</artifactId>
		<version>${spark.version}</version>
	</dependency>
	<dependency>
		<groupId>org.apache.spark</groupId>
		<artifactId>spark-network-shuffle_2.12</artifactId>
		<version>${spark.version}</version>
	</dependency>
	<dependency>
		<groupId>org.apache.spark</groupId>
		<artifactId>spark-streaming_2.12</artifactId>
		<version>${spark.version}</version>
	</dependency>

    <!-- Test -->
    <dependency>
      <groupId>junit</groupId>
      <artifactId>junit</artifactId>
      <version>4.12</version>
      <scope>test</scope>
    </dependency>
    <dependency>
      <groupId>org.scalatest</groupId>
      <artifactId>scalatest_${scala.compat.version}</artifactId>
      <version>3.0.5</version>
      <scope>test</scope>
    </dependency>
    <dependency>
      <groupId>org.specs2</groupId>
      <artifactId>specs2-core_${scala.compat.version}</artifactId>
      <version>${spec2.version}</version>
      <scope>test</scope>
    </dependency>
    <dependency>
      <groupId>org.specs2</groupId>
      <artifactId>specs2-junit_${scala.compat.version}</artifactId>
      <version>${spec2.version}</version>
      <scope>test</scope>
    </dependency>
  </dependencies>

  <build>
    <sourceDirectory>src/main/scala</sourceDirectory>
    <testSourceDirectory>src/test/scala</testSourceDirectory>
    <plugins>
      <plugin>
        <!-- see http://davidb.github.com/scala-maven-plugin -->
        <groupId>net.alchim31.maven</groupId>
        <artifactId>scala-maven-plugin</artifactId>
        <version>3.3.2</version>
        <executions>
          <execution>
            <goals>
              <goal>compile</goal>
              <goal>testCompile</goal>
            </goals>
            <configuration>
              <args>
                <arg>-dependencyfile</arg>
                <arg>${project.build.directory}/.scala_dependencies</arg>
              </args>
            </configuration>
          </execution>
        </executions>
      </plugin>
      <plugin>
        <groupId>org.apache.maven.plugins</groupId>
        <artifactId>maven-surefire-plugin</artifactId>
        <version>2.21.0</version>
        <configuration>
          <!-- Tests will be run with scalatest-maven-plugin instead -->
          <skipTests>true</skipTests>
        </configuration>
      </plugin>
      <plugin>
        <groupId>org.scalatest</groupId>
        <artifactId>scalatest-maven-plugin</artifactId>
        <version>2.0.0</version>
        <configuration>
          <reportsDirectory>${project.build.directory}/surefire-reports</reportsDirectory>
          <junitxml>.</junitxml>
          <filereports>TestSuiteReport.txt</filereports>
          <!-- Comma separated list of JUnit test class names to execute -->
          <jUnitClasses>samples.AppTest</jUnitClasses>
        </configuration>
        <executions>
          <execution>
            <id>test</id>
            <goals>
              <goal>test</goal>
            </goals>
          </execution>
        </executions>
      </plugin>
    </plugins>
  </build>
</project>


