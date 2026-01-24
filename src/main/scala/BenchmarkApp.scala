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
  val concurrencyLevels = Seq(5, 10, 15, 25, 50) 
  val faultModes = Seq("none", "latency", "region_failure", "network_partition")

  def main(args: Array[String]): Unit = {
    // 1. SETUP HADOOP PORTABIL (Critic pentru Windows)
    setupHadoopOnWindows()

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
  }

  // --- SOLUȚIA PORTABILĂ PENTRU WINDOWS ---
  def setupHadoopOnWindows(): Unit = {
    if (System.getProperty("os.name").toLowerCase.contains("win")) {
      try {
        // 1. Găsim calea absolută către folderul 'hadoop' din proiect
        val projectRoot = new File(".").getAbsolutePath
        val hadoopDir = new File(projectRoot, "hadoop")
        val hadoopBin = new File(hadoopDir, "bin")
        val hadoopDll = new File(hadoopBin, "hadoop.dll")

        if (hadoopDir.exists() && hadoopDll.exists()) {
          // 2. Setăm variabila HADOOP_HOME
          System.setProperty("hadoop.home.dir", hadoopDir.getAbsolutePath)
          
          // 3. TRUCUL MAGIC: Încărcăm DLL-ul manual! 
          // Asta rezolvă "UnsatisfiedLinkError" fără a copia fișierul în System32.
          System.load(hadoopDll.getAbsolutePath)
          
          println(f"[WINDOWS CONFIG] Successfully loaded Hadoop DLL from: ${hadoopDll.getAbsolutePath}")
        } else {
          println("[WARNING] 'hadoop/bin/hadoop.dll' not found in project. Windows execution might fail.")
        }
      } catch {
        case e: UnsatisfiedLinkError => 
          println("[WARNING] Failed to load hadoop.dll manually. It might already be loaded or in System32.")
        case e: Exception =>
          println(f"[WARNING] Error setting up Hadoop: ${e.getMessage}")
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
    implicit val ec = ExecutionContext.fromExecutorService(Executors.newFixedThreadPool(writers))
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