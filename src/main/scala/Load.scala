import org.apache.spark.sql.{SparkSession, DataFrame}

object Load {
  def loadToMySQL(spark: SparkSession, df: DataFrame, tableName: String, url: String, user: String, password: String): Unit = {
    // Propriétés pour la connexion MySQL
    val connectionProperties = new java.util.Properties()
    connectionProperties.put("user", user)
    connectionProperties.put("password", password)
    connectionProperties.put("driver", "com.mysql.cj.jdbc.Driver")

    // Écrire le DataFrame dans MySQL
    df.write
      .mode("overwrite")
      .jdbc(url, tableName, connectionProperties)

    println(s"Data loaded successfully into MySQL table: $tableName")
  }
}
