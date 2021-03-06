/*
 * Copyright (C) 2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projection.eventsourced.javadsl

import java.util.Optional
import java.util.concurrent.CompletionStage
import java.util.function.Supplier

import scala.compat.java8.FutureConverters._
import scala.concurrent.ExecutionContext
import scala.concurrent.Future

import akka.NotUsed
import akka.actor.typed.ActorSystem
import akka.annotation.ApiMayChange
import akka.annotation.InternalApi
import akka.persistence.query.NoOffset
import akka.persistence.query.Offset
import akka.persistence.query.PersistenceQuery
import akka.persistence.query.javadsl.EventsByTagQuery
import akka.projection.eventsourced.EventEnvelope
import akka.projection.javadsl
import akka.projection.javadsl.SourceProvider
import akka.stream.javadsl.Source

@ApiMayChange
object EventSourcedProvider {

  def eventsByTag[Event](
      system: ActorSystem[_],
      readJournalPluginId: String,
      tag: String): SourceProvider[Offset, EventEnvelope[Event]] = {

    val eventsByTagQuery =
      PersistenceQuery(system).getReadJournalFor(classOf[EventsByTagQuery], readJournalPluginId)

    new EventsByTagSourceProvider(system, eventsByTagQuery, tag)
  }

  /**
   * INTERNAL API
   */
  @InternalApi
  private class EventsByTagSourceProvider[Event](
      system: ActorSystem[_],
      eventsByTagQuery: EventsByTagQuery,
      tag: String)
      extends javadsl.SourceProvider[Offset, EventEnvelope[Event]] {
    implicit val executionContext: ExecutionContext = system.executionContext

    override def source(offsetAsync: Supplier[CompletionStage[Optional[Offset]]])
        : CompletionStage[Source[EventEnvelope[Event], NotUsed]] = {
      val source: Future[Source[EventEnvelope[Event], NotUsed]] = offsetAsync.get().toScala.map { offsetOpt =>
        eventsByTagQuery
          .eventsByTag(tag, offsetOpt.orElse(NoOffset))
          .map(env => EventEnvelope(env))
      }
      source.toJava
    }

    override def extractOffset(envelope: EventEnvelope[Event]): Offset = envelope.offset

    override def extractCreationTime(envelope: EventEnvelope[Event]): Long = envelope.timestamp
  }
}
