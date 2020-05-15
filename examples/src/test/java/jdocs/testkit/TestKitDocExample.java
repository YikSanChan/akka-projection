/*
 * Copyright (C) 2020 Lightbend Inc. <https://www.lightbend.com>
 */

package jdocs.testkit;
import akka.Done;
import akka.actor.ClassicActorSystemProvider;
import akka.projection.Projection;
import akka.projection.ProjectionId;
import akka.projection.ProjectionSettings;
import akka.projection.RunningProjection;
import akka.stream.scaladsl.Source;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;


//#testkit-import
import org.junit.ClassRule;
import akka.actor.testkit.typed.javadsl.TestKitJunitResource;
import akka.projection.testkit.javadsl.ProjectionTestKit;

//#testkit-import

//#testkit-duration
import java.time.Duration;
import java.util.concurrent.TimeUnit;

//#testkit-duration

//#testkit-assertion-import
import akka.stream.testkit.TestSubscriber;
import akka.actor.testkit.typed.javadsl.TestProbe;
import static org.junit.Assert.assertEquals;

//#testkit-assertion-import

public class TestKitDocExample {

  static class CartView {
    final String id;

    CartView(String id) {
      this.id = id;
    }
  }
  static class CartCheckoutRepository {
    public CompletionStage<CartView> findById(String id) {
      return CompletableFuture.completedFuture(new CartView(id));
    }
  }

  //#testkit
  @ClassRule
  static final TestKitJunitResource testKit = new TestKitJunitResource();
  ProjectionTestKit projectionTestKit = ProjectionTestKit.create(testKit.testKit());
  //#testkit



  Projection<String> projection = new Projection<String>() {
    @Override
    public ProjectionId projectionId() {
      return null;
    }

    @Override
    public Projection<String> withSettings(ProjectionSettings settings) {
      return null;
    }

    @Override
    public Source<Done, ?> mappedSource(ClassicActorSystemProvider systemProvider) {
      return null;
    }

    @Override
    public RunningProjection run(ClassicActorSystemProvider systemProvider) {
      return null;
    }
  };

  CartCheckoutRepository cartCheckoutRepository = new CartCheckoutRepository();

  void illustrateTestKitRun() {
    //#testkit-run
    CartView cartView =
      projectionTestKit.run(projection, () -> {
        return cartCheckoutRepository
                .findById("abc-def")
                .toCompletableFuture().get(1,TimeUnit.SECONDS);
    });
    assertEquals("abc-def", cartView.id);
    //#testkit-run
  }

  void illustrateTestKitRunWithMaxAndInterval() {
    //#testkit-run-max-interval
    CartView cartView =
      projectionTestKit.run(projection, Duration.ofSeconds(5), Duration.ofMillis(300), () -> {
        return cartCheckoutRepository
          .findById("abc-def")
          .toCompletableFuture().get(1, TimeUnit.SECONDS);
      });
    assertEquals("abc-def", cartView.id);
    //#testkit-run-max-interval
  }


  void illustrateTestKitRunWithTestSink() {

    //#testkit-sink-probe
    projectionTestKit.runWithTestSink(projection, sinkProbe -> {

      sinkProbe.request(1);
      sinkProbe.expectNext(Done.getInstance());
      sinkProbe.cancel();

      CartView cartView =
        cartCheckoutRepository
          .findById("abc-def")
          .toCompletableFuture().get(1, TimeUnit.SECONDS);
      assertEquals("abc-def", cartView.id);

      return cartView;
    });

    //#testkit-sink-probe
  }

  //#fixme
  //FIXME: Java example
  //#fixme
}
