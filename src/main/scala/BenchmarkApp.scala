import org.apache.spark.sql.SparkSession
import io.delta.tables.DeltaTable
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.Row
import scala.concurrent.{Future, Await, ExecutionContext}
import scala.concurrent.duration._
import java.util.concurrent.Executors
import scala.util.Random
import java.io.{File, PrintWriter, FileOutputStream}
import java.time.LocalDateTime
import scala.io.Source
import java.util.Properties

object BenchmarkApp {

  var storageAccountName: String = _
  var storageAccountKey: String = _
  var containerName: String = _
  var deltaTablePath: String = _
  
  val csvFile = "results/benchmark_results_scala.csv"
  val concurrencyLevels = Seq(5, 10, 15) 
  val faultModes = Seq("none", "latency", "region_failure", "network_partition")

  def main(args: Array[String]): Unit = {
    // 1. SETUP HADOOP PORTABIL (Critic pentru Windows)
    setupHadoopOnWindows()
    val isWindows = System.getProperty("os.name").toLowerCase.contains("win")

    println("--- INITIALIZING CONFIGURATION ---")
    loadEnvVariables()

    if (storageAccountName == null || storageAccountKey == null || containerName == null) {
      System.err.println("\n[CRITICAL ERROR] Environment variables are NULL.")
      System.err.println("Please create a .env file based on .env.template")
      System.exit(1)
    }

    setupCsv()

    println("--- STARTING SPARK SESSION ---")
    val spark = SparkSession.builder()
      .appName("DeltaOCC_Thesis")
      .master("local[*]") 
      .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
      .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
      .config("spark.databricks.delta.logStore.class", "org.apache.spark.sql.delta.storage.AzureLogStore")
      .config("spark.delta.logStore.cache.enabled", "false")
      .config(f"fs.azure.account.key.$storageAccountName.dfs.core.windows.net", storageAccountKey)
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    println(f"--- BENCHMARK STARTED [Account: $storageAccountName] ---")

    ensureTableExists(spark)

    for (mode <- faultModes) {
      for (writers <- concurrencyLevels) {
        runScenario(spark, writers, mode)
      }
    }

    spark.stop()
    println("\n[DONE] All benchmarks complete.")

    // When running via `sbt run` with `fork := true` on Windows, lingering non-daemon threads (Spark/Hadoop/Azure FS)
    // can keep the forked JVM alive even after the main method finishes.
    if (isWindows) System.exit(0)
  }

  // --- SOLUȚIA PORTABILĂ PENTRU WINDOWS ---
  def setupHadoopOnWindows(): Unit = {
    if (System.getProperty("os.name").toLowerCase.contains("win")) {
      val alreadySet = Option(System.getProperty("hadoop.home.dir")).map(_.trim).filter(_.nonEmpty)
      val fromEnv = Option(System.getenv("HADOOP_HOME")).map(_.trim).filter(_.nonEmpty)

      // Prefer explicit user configuration, then HADOOP_HOME, then a bundled ./hadoop folder.
      val projectRoot = new File(".").getAbsoluteFile
      val bundledHadoopHome = new File(projectRoot, "hadoop")
      val candidateHome = alreadySet
        .orElse(fromEnv)
        .orElse(if (bundledHadoopHome.exists()) Some(bundledHadoopHome.getAbsolutePath) else None)

      candidateHome match {
        case Some(homePath) =>
          val homeDir = new File(homePath)
          val binDir = new File(homeDir, "bin")
          val winutils = new File(binDir, "winutils.exe")
          val hadoopDll = new File(binDir, "hadoop.dll")
          System.setProperty("hadoop.home.dir", homeDir.getAbsolutePath)

          if (!winutils.exists()) {
            System.err.println("\n[CRITICAL ERROR] Windows requires Hadoop winutils.exe for Spark/Hadoop file operations.")
            System.err.println(f"hadoop.home.dir is set to: ${homeDir.getAbsolutePath}")
            System.err.println("Expected to find: <hadoop.home.dir>/bin/winutils.exe")
            System.err.println("\nFix options:")
            System.err.println("  1) Create ./hadoop/bin and place winutils.exe there (recommended for portability)")
            System.err.println("  2) Or set HADOOP_HOME to a folder that contains bin/winutils.exe")
            System.err.println("\nNote: Your project uses Hadoop 3.3.4 (see build.sbt), so match winutils accordingly.")
            System.exit(1)
          } else {
            println(f"[WINDOWS CONFIG] Using Hadoop home: ${homeDir.getAbsolutePath}")
            println(f"[WINDOWS CONFIG] Found winutils.exe at: ${winutils.getAbsolutePath}")
          }

          if (!hadoopDll.exists()) {
            System.err.println("\n[CRITICAL ERROR] Hadoop native library (hadoop.dll) not found.")
            System.err.println("Spark/Hadoop on Windows often requires <hadoop.home.dir>/bin/hadoop.dll to provide JNI methods (e.g. NativeIO$Windows.access0).")
            System.err.println(f"Expected to find: ${hadoopDll.getAbsolutePath}")
            System.err.println("\nFix options:")
            System.err.println("  1) Place a Hadoop 3.3.4-compatible hadoop.dll in ./hadoop/bin (portable)")
            System.err.println("  2) Or point HADOOP_HOME to a Hadoop home that includes bin/hadoop.dll")
            System.exit(1)
          }

          // Force-load the correct DLL early so Hadoop doesn't pick up an incompatible hadoop.dll from PATH/System32.
          try {
            System.load(hadoopDll.getAbsolutePath)
            println(f"[WINDOWS CONFIG] Loaded hadoop.dll from: ${hadoopDll.getAbsolutePath}")
          } catch {
            case e: UnsatisfiedLinkError =>
              System.err.println("\n[CRITICAL ERROR] Failed to load hadoop.dll.")
              System.err.println(f"Attempted: ${hadoopDll.getAbsolutePath}")
              System.err.println(f"Reason: ${e.getMessage}")
              System.err.println("\nThis usually means the DLL is missing dependencies (e.g., MSVC runtime) or a different hadoop.dll version was loaded earlier from PATH.")
              System.err.println("Try running from a fresh shell and ensure no other hadoop.dll is present in PATH/System32.")
              System.exit(1)
          }

        case None =>
          System.err.println("\n[CRITICAL ERROR] HADOOP_HOME / hadoop.home.dir is not set on Windows.")
          System.err.println("Spark on Windows needs a Hadoop home containing bin/winutils.exe.")
          System.err.println("\nFix options:")
          System.err.println("  1) Add winutils.exe under ./hadoop/bin and rerun")
          System.err.println("  2) Or set HADOOP_HOME to a folder that contains bin/winutils.exe")
          System.err.println("\nNote: Your project uses Hadoop 3.3.4 (see build.sbt), so match winutils accordingly.")
          System.exit(1)
      }
    }
  }

  def loadEnvVariables(): Unit = {
    val envFile = new File(".env")
    val props = new Properties()
    
    if (envFile.exists()) {
      val source = Source.fromFile(envFile)
      try {
        for (line <- source.getLines()) {
          val trimmed = line.trim
          if (trimmed.nonEmpty && !trimmed.startsWith("#") && trimmed.contains("=")) {
            val parts = trimmed.split("=", 2)
            props.setProperty(parts(0).trim, parts(1).trim)
          }
        }
      } finally {
        source.close()
      }
    } else {
       println("WARNING: .env file not found. Trying system environment variables.")
    }

    def getEnv(key: String): String = {
      Option(props.getProperty(key)).getOrElse(sys.env.getOrElse(key, null))
    }

    storageAccountName = getEnv("STORAGE_ACCOUNT_NAME")
    storageAccountKey = getEnv("AZURE_ACCESS_KEY") 
    containerName = getEnv("CONTAINER_NAME")
    
    if (storageAccountName != null && containerName != null) {
        deltaTablePath = f"abfss://$containerName@$storageAccountName.dfs.core.windows.net/sensor_resilience_table"
    }
  }

  def runScenario(spark: SparkSession, writers: Int, mode: String): Unit = {
    println(f"\n>>> SCENARIO: Writers=$writers, Fault=$mode")
    val executor = Executors.newFixedThreadPool(writers)
    implicit val ec: ExecutionContext = ExecutionContext.fromExecutorService(executor)

    try {
      val startTime = System.currentTimeMillis()
      val futures = (1 to writers).map { id => Future { executeWriter(spark, id, mode) } }
      val combined = Future.sequence(futures)
      val results = Await.result(combined, 5.minutes)
      val durationMs = System.currentTimeMillis() - startTime
      val success = results.count(_ == "Success")
      val conflicts = results.count(_ == "Conflict")
      val failures = results.count(_ == "Fault/Error")
      val conflictRate = if (writers > 0) conflicts.toDouble / writers else 0.0
      println(f"     Results: Success=$success, Conflicts=$conflicts, Failures=$failures")
      appendToCsv(writers, mode, success, conflicts, failures, conflictRate, durationMs)
    } finally {
      // Important: the thread pool uses non-daemon threads; if not shut down, the JVM will hang after [DONE].
      executor.shutdown()
      executor.awaitTermination(30, java.util.concurrent.TimeUnit.SECONDS)
    }
  }

  def executeWriter(spark: SparkSession, id: Int, mode: String): String = {
    try {
      Thread.sleep(Random.nextInt(500)) 
      if (mode == "latency") { Thread.sleep(200 + Random.nextInt(800)) }
      if (mode == "network_partition" && id % 2 == 0) { Thread.sleep(5000); throw new RuntimeException("Network Timeout") }
      if (mode == "region_failure") { throw new java.net.UnknownHostException(f"$storageAccountName.dfs.core.windows.net") }

      val dt = DeltaTable.forPath(spark, deltaTablePath)
      dt.update(col("id") === 999, Map("value" -> lit(Random.nextDouble() * 100)))
      "Success"
    } catch {
      case e: Exception if e.getMessage != null && (e.getMessage.contains("ConcurrentModification") || e.getMessage.contains("concurrent update") || e.toString.contains("DeltaConcurrentModificationException")) => "Conflict"
      case e: java.net.UnknownHostException => "Fault/Error"
      case e: RuntimeException if e.getMessage == "Network Timeout" => "Fault/Error"
      case e: Exception => "Fault/Error"
    }
  }

  def setupCsv(): Unit = {
    val f = new File(csvFile)
    if (f.getParentFile != null) f.getParentFile.mkdirs()
    if (!f.exists()) { val pw = new PrintWriter(f); pw.write("timestamp,writers,fault_mode,success,conflicts,failures,conflict_rate,duration_ms\n"); pw.close() }
  }

  def appendToCsv(w: Int, m: String, s: Int, c: Int, f: Int, r: Double, d: Long): Unit = {
    val pw = new PrintWriter(new FileOutputStream(new File(csvFile), true)); val ts = LocalDateTime.now().toString; pw.write(f"$ts,$w,$m,$s,$c,$f,$r%.2f,$d\n"); pw.close()
  }

  def ensureTableExists(spark: SparkSession): Unit = {
    println(f"--- FORCING TABLE RESET AT $deltaTablePath ---")
    val schema = StructType(Seq(StructField("id", LongType, true), StructField("value", DoubleType, true), StructField("timestamp", TimestampType, true)))
    import spark.implicits._
    val seedData = Seq((999L, 100.0, java.sql.Timestamp.valueOf(java.time.LocalDateTime.now())))
    val df = spark.createDataFrame(spark.sparkContext.parallelize(seedData).map(r => Row(r._1, r._2, r._3)), schema)
    println("Overwriting existing table with clean data...")
    df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(deltaTablePath)
    println("Table has been successfully reset and seeded.")
  }
}