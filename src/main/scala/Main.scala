import os._
import scala.concurrent.duration._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SaveMode

object Main extends App {

  // Configuration du niveau de logging pour Spark et Akka
  Logger.getLogger("org").setLevel(Level.ERROR)
  Logger.getLogger("akka").setLevel(Level.ERROR)
  Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)
  Logger.getLogger("org.spark-project").setLevel(Level.ERROR)

  // Définition des équipes et de leurs identifiants
  val teams: Map[String, Int] = Map(
    "Phoenix Suns" -> 24,
    "Atlanta Hawks" -> 1,
    "Los Angeles Lakers" -> 14,
    "Milwaukee Bucks" -> 17
  )

  // Chemins de sortie pour les matchs et les statistiques
  val gamesOutputPath = os.pwd / "games"
  val statsOutputPath = os.pwd / "stats"

  
  // Extraction et sauvegarde des statistiques des équipes
  teams.foreach { case (teamName, teamId) =>
    val gameIds = Extract.extract_game_ids(teamId)
    val statsData = Extract.extract_team_stats(teamId, gameIds)
    Extract.save_stats_data(teamName, statsData, statsOutputPath)
  }
  
  // Extraction et sauvegarde des matchs des équipes
  teams.foreach { case (teamName, teamId) =>
    val gamesData = Extract.extract_team_games(teamId)
    Extract.save_games_data(teamName, gamesData, gamesOutputPath)
  }
  

  
  // Traitement des répertoires de matchs et de statistiques
  val baseDir = os.pwd 
  val directories = List("games", "stats")

  directories.foreach { dir =>
    val dirPath = baseDir / dir
    println(s"Traitement du répertoire: $dirPath")
    Transform.processDirectory(dirPath)
  }

  println("Transformation terminée.")
  

  // Initialisation de Spark
  val spark = SparkSession.builder()
    .appName("NBA Data Processing")
    .master("local[*]")
    .config("spark.ui.showConsoleProgress", "false")
    .getOrCreate()

  // Création des DataFrames pour les jeux et les statistiques
  val gamesdf = Transform.createGamesDataFrame(spark, gamesOutputPath)
  val statsdf = Transform.createStatsDataFrame(spark, statsOutputPath) 

  // Fusion des DataFrames sur l'identifiant de match
  val mergeddf = gamesdf.join(statsdf, Seq("match_id"), "inner")

  // Sélection des colonnes souhaitées
  val resultdf = mergeddf.select(
    col("match_id"),
    col("team_id_1").as("team_id"), 
    col("team_name_1").as("team_name"),  
    col("points").as("game_points"),
    col("total_pts"),
    col("total_reb"),
    col("total_ast"),
    col("total_blk"),
    col("best_scorer"),
    col("best_rebounder"),
    col("best_assist"),
    col("best_blocker")
  )

  println("Résultat final :")
  resultdf.show()

  // Définition du chemin pour le fichier CSV de sortie
  val currentDir = os.pwd
  val csvDir = currentDir / "csv"
  val csvPath = csvDir / "nba_data.csv"

  // Assurez-vous que le dossier csv existe
  if (!os.exists(csvDir)) {
    os.makeDir(csvDir)
  }

  // Sauvegarde du DataFrame en un seul fichier CSV
  resultdf.coalesce(1) // Pour s'assurer qu'il n'y a qu'un seul fichier en sortie
    .write
    .mode(SaveMode.Overwrite) // Écrase le fichier s'il existe déjà
    .option("header", "true")
    .csv(csvDir.toString)

  // Renommer le fichier généré
  val generatedFiles = os.list(csvDir)
  val csvFile = generatedFiles.find(_.ext == "csv").get
  os.move(csvFile, csvPath, replaceExisting = true)

  // Supprimer les fichiers supplémentaires générés par Spark
  generatedFiles.foreach { file =>
    if (file.ext != "csv") {
      os.remove(file)
    }
  }

  println(s"DataFrame sauvegardé dans : $csvPath")

  // Paramètres de connexion MySQL
  val mysqlUrl = "jdbc:mysql://localhost:3306/NBA_INFOS"
  val mysqlUser = "nba_user"
  val mysqlPassword = "password_nba"
  val mysqlTable = "nba_data"

  // Chargement du DataFrame dans MySQL
  Load.loadToMySQL(spark, resultdf, mysqlTable, mysqlUrl, mysqlUser, mysqlPassword)

  // Arrêt de Spark
  spark.stop()
}
