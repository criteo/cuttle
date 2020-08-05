package com.criteo.cuttle.cron

import io.circe._
import io.circe.generic.semiauto._

private[cron] object SortQuery {
  implicit val decodeSortQuery: Decoder[SortQuery] = deriveDecoder[SortQuery]
  implicit val encodeSortQuery: Encoder[SortQuery] = deriveEncoder[SortQuery]
}

private[cron] case class SortQuery(
  column: String,
  order: String
) {
  val asc = order.toLowerCase == "asc"
}

private[cron] object ExecutionsQuery {
  implicit val decodeExecutionsParams: Decoder[ExecutionsQuery] = deriveDecoder[ExecutionsQuery]
}
private[cron] case class ExecutionsQuery(
  jobs: Set[String],
  sort: SortQuery,
  limit: Int,
  offset: Int
) {
  def jobIds(allIds: Set[String]) = if (jobs.isEmpty) allIds else jobs
}
