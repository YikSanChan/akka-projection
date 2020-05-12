/*
 * Copyright (C) 2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projection.cassandra.scaladsl

import scala.concurrent.duration.Duration
import scala.concurrent.Future
import scala.concurrent.duration.FiniteDuration

import akka.Done
import akka.actor.ClassicActorSystemProvider
import akka.annotation.ApiMayChange
import akka.projection.Projection
import akka.projection.ProjectionId
import akka.projection.ProjectionSettings
import akka.projection.cassandra.internal.CassandraProjectionImpl
import akka.projection.scaladsl.GroupedHandler
import akka.projection.scaladsl.Handler
import akka.projection.scaladsl.SourceProvider

/**
 * Factories of [[Projection]] where the offset is stored in Cassandra. The envelope handler can
 * integrate with anything, such as publishing to a message broker, or updating a read model in Cassandra.
 *
 * The envelope handler function can be stateful, with variables and mutable data structures.
 * It is invoked by the `Projection` machinery one envelope at a time and visibility
 * guarantees between the invocations are handled automatically, i.e. no volatile or
 * other concurrency primitives are needed for managing the state.
 */
@ApiMayChange
object CassandraProjection {
  import CassandraProjectionImpl.{ AtLeastOnce, AtMostOnce }
  import CassandraProjectionImpl.{ GroupedHandlerStrategy, SingleHandlerStrategy }

  /**
   * Create a [[Projection]] with at-least-once processing semantics. It stores the offset in Cassandra
   * after the `handler` has processed the envelope. This means that if the projection is restarted
   * from previously stored offset some envelopes may be processed more than once.
   */
  def atLeastOnce[Offset, Envelope](
      projectionId: ProjectionId,
      sourceProvider: SourceProvider[Offset, Envelope],
      saveOffsetAfterEnvelopes: Int,
      saveOffsetAfterDuration: FiniteDuration,
      handler: Handler[Envelope]): CassandraProjection[Envelope] =
    new CassandraProjectionImpl(
      projectionId,
      sourceProvider,
      settingsOpt = None,
      offsetStrategy = AtLeastOnce(saveOffsetAfterEnvelopes, saveOffsetAfterDuration),
      handlerStrategy = SingleHandlerStrategy(handler))

  /**
   * Create a [[Projection]] that groups envelopes and calls the `handler` with a group of `Envelopes`.
   * It stores the offset in Cassandra immediately after the `handler` has processed the envelopes, but that
   * is still with at-least-once processing semantics. This means that if the projection is restarted
   * from previously stored offset the previous group of envelopes may be processed more than once.
   */
  def grouped[Offset, Envelope](
      projectionId: ProjectionId,
      sourceProvider: SourceProvider[Offset, Envelope],
      groupAfterEnvelopes: Int,
      groupAfterDuration: FiniteDuration,
      handler: GroupedHandler[Envelope]): Projection[Envelope] =
    new CassandraProjectionImpl(
      projectionId,
      sourceProvider,
      settingsOpt = None,
      offsetStrategy = AtLeastOnce(1, Duration.Zero),
      handlerStrategy = GroupedHandlerStrategy(handler, groupAfterEnvelopes, groupAfterDuration))

  /**
   * Create a [[Projection]] with at-most-once processing semantics. It stores the offset in Cassandra
   * before the `handler` has processed the envelope. This means that if the projection is restarted
   * from previously stored offset one envelopes may not have been processed.
   */
  def atMostOnce[Offset, Envelope](
      projectionId: ProjectionId,
      sourceProvider: SourceProvider[Offset, Envelope],
      handler: Handler[Envelope]): CassandraProjection[Envelope] =
    new CassandraProjectionImpl(
      projectionId,
      sourceProvider,
      settingsOpt = None,
      offsetStrategy = AtMostOnce,
      handlerStrategy = SingleHandlerStrategy(handler))
}

trait CassandraProjection[Envelope] extends Projection[Envelope] {

  override def withSettings(settings: ProjectionSettings): CassandraProjection[Envelope]

  /**
   * For testing purposes the offset table can be created programmatically.
   * For production it's recommended to create the table with DDL statements
   * before the system is started.
   */
  def createOffsetTableIfNotExists()(implicit systemProvider: ClassicActorSystemProvider): Future[Done]
}
