package org.tikv.common.exception;

public class GCException extends TiKVException {

  private static final long serialVersionUID = 7005512814023990958L;

  public GCException(Throwable e) {
    super(e);
  }

  public GCException(String msg) {
    super(msg);
  }

  public GCException(String msg, Throwable e) {
    super(msg, e);
  }
}