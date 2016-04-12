package scalapgq

import scalikejdbc._
import com.typesafe.config.ConfigFactory

trait PGQSpec {
  GlobalSettings.loggingSQLAndTime = LoggingSQLAndTimeSettings(enabled = true, singleLineMode = true)
  val config = ConfigFactory.load()
  
  val PostgresUrl = config.getString("db.url")
  val PostgresUser = config.getString("db.user")
  val PostgresPassword = config.getString("db.password")
}