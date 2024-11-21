package com.videoapi.repository

import com.videoapi.schema.VideoUrl
import com.videoapi.schema.VideoUrlRepository.videoUrls
import slick.jdbc.PostgresProfile.api._
import scala.concurrent.{ExecutionContext, Future}

class VideoUrlRepository(db: Database)(implicit ec: ExecutionContext) {

  // Fetch URLs for multiple video IDs
  def getUrlsByVideoIds(videoIds: List[String]): Future[Seq[VideoUrl]] = {
    if (videoIds.isEmpty) {
      // Return an empty sequence or handle accordingly if no video IDs are provided
      Future.successful(Seq.empty)
    } else {
      val query = videoUrls.filter(_.videoId.inSet(videoIds))
      Thread.sleep(3000)
      println(s"Running query: $query with videoIds: $videoIds") // Log query details
      db.run(query.result)
    }
  }

  // Fetch a single VideoUrl by its video ID
  def getUrlByVideoId(videoId: String): Future[Option[VideoUrl]] = {
    // Query to find a VideoUrl by videoId
    val query = videoUrls.filter(_.videoId === videoId).result.headOption

    // Run the query and return the result wrapped in a Future
    db.run(query).map { result =>
      // Map the result to an Option[VideoUrl]
      result
    }
  }
}
