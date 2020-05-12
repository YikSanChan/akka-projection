/*
 * Copyright (C) 2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projection.internal

import scala.concurrent.Future
import scala.util.control.NonFatal

import akka.Done
import akka.actor.ClassicActorSystemProvider
import akka.annotation.InternalApi
import akka.event.LoggingAdapter
import akka.pattern.after
import akka.pattern.retry
import akka.projection.HandlerRecovery
import akka.projection.HandlerRecoveryStrategy

/**
 * INTERNAL API
 */
@InternalApi private[akka] object HandlerRecoveryImpl {

  private val futDone = Future.successful(Done)

  def applyUserRecovery[Offset, Envelope](
      handler: HandlerRecovery[Envelope],
      envelope: Envelope,
      firstOffset: Offset, // used for logging
      lastOffset: Offset, // used for logging
      logger: LoggingAdapter,
      futureCallback: () => Future[Done])(implicit systemProvider: ClassicActorSystemProvider): Future[Done] = {
    import HandlerRecoveryStrategy.Internal._

    implicit val scheduler = systemProvider.classicSystem.scheduler
    implicit val dispatcher = systemProvider.classicSystem.dispatcher

    val tryFutureCallback: () => Future[Done] = { () =>
      try {
        futureCallback()
      } catch {
        case NonFatal(e) =>
          // in case the callback throws instead of returning failed Future
          Future.failed(e)
      }
    }

    // this will count as one attempt
    val firstAttempt = tryFutureCallback()

    def offsetLogParameter: String =
      if (firstOffset == lastOffset) s"envelope with offset [$firstOffset]"
      else s"envelopes with offsets from [$firstOffset] to [$lastOffset]"

    firstAttempt.recoverWith {
      case err =>
        handler.onFailure(envelope, err) match {
          case Fail =>
            logger.error(
              cause = err,
              template = "Failed to process {}. Projection will stop as defined by recovery strategy.",
              offsetLogParameter)
            firstAttempt

          case Skip =>
            logger.warning(
              "Failed to process {}. " +
              "Envelope will be skipped as defined by recovery strategy. Exception: {}",
              offsetLogParameter,
              err)
            futDone

          case RetryAndFail(retries, delay) =>
            logger.warning(
              "First attempt to process {} failed. Will retry [{}] time(s). " +
              "Exception: {}",
              offsetLogParameter,
              retries,
              err)

            // retries - 1 because retry() is based on attempts
            // first attempt is performed immediately and therefore we must first delay
            val retried = after(delay, scheduler)(retry(tryFutureCallback, retries - 1, delay))
            retried.failed.foreach { exception =>
              logger.error(
                cause = exception,
                template =
                  "Failed to process {} after [{}] attempts. " +
                  "Projection will stop as defined by recovery strategy.",
                offsetLogParameter,
                retries + 1)
            }
            retried

          case RetryAndSkip(retries, delay) =>
            logger.warning(
              "First attempt to process {} failed. Will retry [{}] time(s). Exception: {}",
              offsetLogParameter,
              retries,
              err)

            // retries - 1 because retry() is based on attempts
            // first attempt is performed immediately and therefore we must first delay
            val retried = after(delay, scheduler)(retry(tryFutureCallback, retries - 1, delay))
            retried.failed.foreach { exception =>
              logger.warning(
                "Failed to process {} after [{}] attempts. " +
                "Envelope will be skipped as defined by recovery strategy. Last exception: {}",
                offsetLogParameter,
                retries + 1,
                exception)
            }
            retried.recoverWith(_ => futDone)
        }
    }
  }

}
