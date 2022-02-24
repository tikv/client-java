package org.tikv.common;

import io.grpc.Status;
import java.util.function.Function;

public class MockRequestInterceptor<I, R> implements Interceptor<I, R> {

  private Throwable error;
  private long latency;
  private Function<I, Throwable> checker;
  private Function<I, R> handler;

  public static class MockRequestInterceptorBuilder<I, R> {

    private final MockRequestInterceptor<I, R> interceptor;

    public MockRequestInterceptorBuilder() {
      interceptor = new MockRequestInterceptor<>();
    }

    public MockRequestInterceptorBuilder<I, R> withError(Throwable error) {
      interceptor.error = error;
      return this;
    }

    public MockRequestInterceptorBuilder<I, R> withLatency(long latency) {
      interceptor.latency = latency;
      return this;
    }

    public MockRequestInterceptorBuilder<I, R> withChecker(Function<I, Throwable> checker) {
      interceptor.checker = checker;
      return this;
    }

    public MockRequestInterceptorBuilder<I, R> withHandler(Function<I, R> handler) {
      interceptor.handler = handler;
      return this;
    }

    public MockRequestInterceptor<I, R> build() {
      return interceptor;
    }
  }

  @Override
  public R apply(I request) throws Throwable {
    if (error != null) {
      throw error;
    }
    if (checker != null) {
      Throwable t = checker.apply(request);
      if (t != null) {
        throw t;
      }
    }
    if (latency > 0) {
      try {
        Thread.sleep(latency);
      } catch (InterruptedException ignored) {
      }
    }
    R resp = handler.apply(request);
    if (resp == null) {
      throw new Throwable(Status.INTERNAL.asRuntimeException());
    }
    return resp;
  }
}
