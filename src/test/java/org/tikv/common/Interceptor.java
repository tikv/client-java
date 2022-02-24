package org.tikv.common;

public interface Interceptor<I, R> {

  R apply(I request) throws Throwable;
}
