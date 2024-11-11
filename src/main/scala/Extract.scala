import scala.concurrent.duration._
import os._

object Extract {
  val games_url: String = "https://api.balldontlie.io/v1/games"
  val stats_url: String = "https://api.balldontlie.io/v1/stats"
  val team_names: List[String] = List("Phoenix Suns", "Atlanta Hawks", "Los Angeles Lakers", "Milwaukee Bucks")
  val api_key: String = "YOUR API KEY"
  val headers: Map[String, String] = Map("Authorization" -> api_key)
  val season = "2023"

  // Fonction pour extraire une page de données depuis une URL
  def extract_page(url: String): ujson.Value.Value = {
    val result: requests.Response = requests.get(url, headers = headers)
    println(s"Code de statut : ${result.statusCode}")
    /*println(s"Réponse brute : ${result.text}")*/
    val ujs: ujson.Value.Value = ujson.read(result.text)
    ujs
  }

  // Fonction pour extraire toutes les pages de données disponibles
  def extract_pages_data(url: String, delay: FiniteDuration = 2.seconds): List[ujson.Value] = {
    var cursor: Option[ujson.Value] = None
    var pages_data: List[ujson.Value] = List[ujson.Value]()
    
    do {
      val fullUrl = cursor match {
        case Some(c) => s"$url&cursor=$c"
        case None => url
      }
      
      val ujs: ujson.Value.Value = extract_page(fullUrl)
      
      try {
        if (ujs.obj.contains("data")) {
          val data: ujson.Value = ujs("data")
          pages_data :+= data
        } else {
          println(s"Structure de réponse inattendue : champ 'data' manquant : ${ujson.write(ujs, indent = 2)}")
        }
        
        if (ujs.obj.contains("meta")) {
          val metadata: ujson.Value = ujs("meta")
          cursor = metadata.obj.get("next_cursor")
        } else {
          cursor = None
        }
      } catch {
        case e: Exception =>
          println(s"Erreur lors du traitement de la page : $e")
          cursor = None
      }

      Thread.sleep(delay.toMillis)
    } while (cursor.isDefined && cursor.get != ujson.Null)
    
    if (pages_data.isEmpty) {
      println("Aucune donnée collectée. Vérifiez l'URL de l'API et les paramètres.")
    }
    
    pages_data
  }

  // Fonction pour extraire les IDs des équipes
  def extract_team_ids(url: String, team_names: List[String]): List[Int] = {
    val all_teams_data: List[ujson.Value] = extract_pages_data(url)
    var team_id_map: Map[String, Int] = Map[String, Int]()
    
    for (page_data <- all_teams_data) {
      val teams = page_data.arr
      for (team <- teams) {
        val team_name = team("full_name").str
        if (team_names.contains(team_name)) {
          val team_id = team("id").num.toInt
          team_id_map += (team_name -> team_id)
        }
      }
    }
    
    val team_ids: List[Int] = team_names.map(team_name => team_id_map(team_name))
    team_ids
  }

 // Fonction pour extraire les IDs des matchs d'une équipe pour une saison donnée
  def extract_game_ids(teamId: Int, season: String = "2023"): List[Int] = {
    val url = s"$games_url?seasons[]=$season&team_ids[]=$teamId"
    val gamesData = extract_pages_data(url)
    gamesData.flatMap(_.arr.map(_("id").num.toInt))
  }

  // Fonction pour extraire les statistiques d'une équipe à partir des ids des matchs pour une saison donnée
 def extract_team_stats(teamId: Int, gameIds: List[Int]): List[ujson.Value] = {
  val gameIdGroups = gameIds.grouped(5).toList // Group game IDs in batches of 5 (afin de réduire le nombre de requetes)
  val statsData = gameIdGroups.flatMap { group =>
    val gameIdParams = group.map(gameId => s"game_ids[]=$gameId").mkString("&")
    val params = Map(
      "team_ids[]" -> teamId.toString
    )
    val urlWithParams = s"$stats_url?${params.map { case (k, v) => s"$k=$v" }.mkString("&")}&$gameIdParams"
    extract_pages_data(urlWithParams)
  }
  statsData
}

  // Fonction pour extraire les matchs d'une équipe pour une saison donnée
  def extract_team_games(teamId: Int): List[ujson.Value] = {
    val url = s"$games_url?seasons[]=$season&team_ids[]=$teamId"
    extract_pages_data(url)
  }

  // Fonction pour sauvegarder les données dans un fichier unique pour les statistiques
  def save_stats_data(teamName: String, statsData: List[ujson.Value], outputPath: os.Path): Unit = {
    val fileName = s"${teamName}_stats.json"
    val filePath = outputPath / fileName
    os.write.over(filePath, ujson.write(statsData, indent = 2), createFolders = true)
    println(s"Données de statistiques pour l'équipe $teamName sauvegardées dans ${filePath.toString}")
  }

  // Fonction pour sauvegarder les données dans un fichier unique pour les matchs
  def save_games_data(teamName: String, gamesData: List[ujson.Value], outputPath: os.Path): Unit = {
    val fileName = s"${teamName}_games.json"
    val filePath = outputPath / fileName
    os.write.over(filePath, ujson.write(gamesData, indent = 2), createFolders = true)
    println(s"Données des matchs pour l'équipe $teamName sauvegardées dans ${filePath.toString}")
  }
}
