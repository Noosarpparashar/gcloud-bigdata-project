package com.videoapi.routes

import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Route
import akka.http.scaladsl.model.StatusCodes
import spray.json._
import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport._
import com.videoapi.repository.VideoUrlRepository
import com.videoapi.schema.VideoUrl
import spray.json.DefaultJsonProtocol.seqFormat

import scala.concurrent.ExecutionContext

// JSON format for marshalling/unmarshalling VideoUrl case class
object JsonFormats {
  import DefaultJsonProtocol._

  implicit val videoUrlFormat: RootJsonFormat[VideoUrl] = jsonFormat2(VideoUrl)

  // Define a case class for the incoming request containing videoIds
  case class VideoIdsRequest(videoIds: List[String])
  implicit val videoIdsRequestFormat: RootJsonFormat[VideoIdsRequest] = jsonFormat1(VideoIdsRequest)
}

class VideoUrlRoutes(repository: VideoUrlRepository)(implicit ec: ExecutionContext) {

  import JsonFormats._

  val routes: Route = pathPrefix("api" / "v1" / "videos") {
    path("urls") {
      concat(
        // Handle POST requests to fetch URLs for multiple video IDs
        post {
          entity(as[VideoIdsRequest]) { videoIdsRequest =>
            val videoIds = videoIdsRequest.videoIds
            onSuccess(repository.getUrlsByVideoIds(videoIds)) { result =>
              complete(StatusCodes.OK, result)
            }
          }
        },
        // Handle GET requests to fetch URLs by query parameters
        get {
          parameters("videoIds".as[String]) { videoIdsParam =>
            val videoIds = videoIdsParam.split(",").toList
            onSuccess(repository.getUrlsByVideoIds(videoIds)) { result =>
              complete(StatusCodes.OK, result)
            }
          }

        },

        path(Segment / "urls") { videoId =>
          get {
            onSuccess(repository.getUrlByVideoId(videoId)) { result =>
              complete(StatusCodes.OK, result)
            }
          }
        },
      )
      }
  }
}

