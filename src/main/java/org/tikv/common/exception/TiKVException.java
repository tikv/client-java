package org.tikv.common.exception;

public class TiKVException extends RuntimeException {

  private static final long serialVersionUID = -5162704963942276016L;

  public TiKVException(Throwable e) {
    super(e);
  }

  public TiKVException(String msg) {
    super(msg);
  }

  public TiKVException(String msg, Throwable e) {
    super(msg, e);
  }
}
