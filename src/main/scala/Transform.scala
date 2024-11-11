import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.functions._
import ujson._
import os.{Path, read, write, up, walk}
import org.apache.spark.sql.expressions.Window

object Transform {
  /*
   * Les fichiers JSON de games et stats obtenus sont de la forme
   * [[{data},{data} ...]] ce qui ne nous arrange pas lors de la lecture
   * via Spark, car de la forme de tableaux dans un tableau, or il nous faut
   * un tableau de données JSON. Les fonctions transformJsonFile
   * et processDirectory ont pour but d'aplatir les données
   * et de faire de notre fichier JSON un fichier lisible via Spark.
   */

  def transformJsonFile(path: os.Path): Unit = {
    // Lire le contenu du fichier
    val jsonContent = os.read(path)
    
    // Parser le JSON
    val json = ujson.read(jsonContent)
    
    // Créer le nouveau nom de fichier
    val newPath = path / os.up / s"transformed_${path.last}"
    
    // Tenter de retirer une paire de crochets
    val transformedJson = json match {
      case arr: ujson.Arr if arr.arr.nonEmpty => 
        // Si c'est un tableau, prendre son contenu
        ujson.Arr(arr.arr.flatMap {
          case innerArr: ujson.Arr => innerArr.arr
          case other => Seq(other)
        })
      case other => other // Si ce n'est pas un tableau, le laisser tel quel
    }
    
    // Écrire le JSON transformé
    os.write.over(newPath, ujson.write(transformedJson, indent = 2))
    println(s"Fichier transformé: $newPath")
    
    // Afficher des informations sur la transformation
    println(s"Nombre d'éléments avant: ${if (json.isInstanceOf[ujson.Arr]) json.arr.size else 1}")
    println(s"Nombre d'éléments après: ${if (transformedJson.isInstanceOf[ujson.Arr]) transformedJson.arr.size else 1}")
  }

  def processDirectory(dir: Path): Unit = {
    // Traiter tous les fichiers JSON du répertoire
    os.walk(dir)
      .filter(_.ext == "json")
      .foreach(transformJsonFile)
  }

  def createGamesDataFrame(spark: SparkSession, directory: os.Path): DataFrame = {
    // Lire tous les fichiers JSON transformés
    val df = spark.read.option("multiline", "true").json((directory / "transformed_*_games.json").toString)

    // Liste des équipes que nous voulons
    val targetTeams = Set("Phoenix Suns", "Atlanta Hawks", "Los Angeles Lakers", "Milwaukee Bucks")

    // Extraire les informations pour l'équipe à domicile
    val homeTeamDF = df
      .filter(col("home_team.full_name").isin(targetTeams.toSeq:_*))
      .select(
        col("id").as("match_id"),
        col("home_team.full_name").as("team_name"),
        col("home_team.id").as("team_id"),
        col("home_team_score").as("points")
      )

    // Extraire les informations pour l'équipe visiteuse
    val visitorTeamDF = df
      .filter(col("visitor_team.name").isin(targetTeams.toSeq:_*))
      .select(
        col("id").as("match_id"),
        col("visitor_team.full_name").as("team_name"),
        col("visitor_team.id").as("team_id"),
        col("visitor_team_score").as("points")
      )

    // Combiner les deux DataFrames
    homeTeamDF.union(visitorTeamDF)
  }

  def createStatsDataFrame(spark: SparkSession, directory: os.Path): DataFrame = {
    // Lecture des fichiers JSON
    val df = spark.read
      .option("multiline", "true")
      .json((directory / "transformed_*_stats.json").toString)

    // Définition des équipes cibles
    val targetTeams = Set("Phoenix Suns", "Atlanta Hawks", "Los Angeles Lakers", "Milwaukee Bucks")

    // Extraction des champs imbriqués dans des colonnes individuelles
    val extractedDF = df
      .select(
        col("game.id").as("game_id_1"),
        col("team.id").as("team_id_1"),
        col("team.full_name").as("team_name_1"),
        concat_ws(" ", col("player.first_name"), col("player.last_name")).as("player_name_1"),
        col("pts"),
        col("reb"),
        col("ast"),
        col("blk")
      )

    // Déduplication
    val dedupDF = extractedDF.dropDuplicates("game_id_1", "team_id_1", "player_name_1")

    // Filtrer les équipes cibles
    val filteredDF = dedupDF.filter(col("team_name_1").isin(targetTeams.toSeq: _*))

    // Spécification de la fenêtre pour chaque jeu et équipe
    val windowSpec = Window.partitionBy("game_id_1", "team_id_1")

    // Création des nouvelles colonnes pour les meilleurs joueurs
    val resultDF = filteredDF
      .withColumn("max_pts", max("pts").over(windowSpec))
      .withColumn("max_reb", max("reb").over(windowSpec))
      .withColumn("max_ast", max("ast").over(windowSpec))
      .withColumn("max_blk", max("blk").over(windowSpec))
      .groupBy("game_id_1", "team_id_1", "team_name_1")
      .agg(
        sum("pts").as("total_pts"),
        sum("reb").as("total_reb"),
        sum("ast").as("total_ast"),
        sum("blk").as("total_blk"),
        first(when(col("pts") === col("max_pts"), col("player_name_1")), ignoreNulls = true).as("best_scorer"),
        first(when(col("reb") === col("max_reb"), col("player_name_1")), ignoreNulls = true).as("best_rebounder"),
        first(when(col("ast") === col("max_ast"), col("player_name_1")), ignoreNulls = true).as("best_assist"),
        first(when(col("blk") === col("max_blk"), col("player_name_1")), ignoreNulls = true).as("best_blocker")
      )
      .select(
        col("game_id_1").as("match_id"),
        col("team_id_1"),
        col("team_name_1"),
        col("total_pts"),
        col("total_reb"),
        col("total_ast"),
        col("total_blk"),
        col("best_scorer"),
        col("best_rebounder"),
        col("best_assist"),
        col("best_blocker")
      )

    resultDF
  }
}
