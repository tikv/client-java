package org.tikv.common.pd;

import org.tikv.kvproto.Pdpb;

public final class Error {
  private final Pdpb.Error error;

  private final ErrorType errorType;

  public enum ErrorType {
    PD_ERROR,
    REGION_PEER_NOT_ELECTED
  }

  private Error(Pdpb.Error error, ErrorType errorType) {
    this.error = error;
    this.errorType = errorType;
  }

  public static Builder newBuilder() {
    return new Builder();
  }

  public static Builder newBuilder(Pdpb.Error error) {
    return new Builder(error);
  }

  public Pdpb.Error getError() {
    return error;
  }

  public ErrorType getErrorType() {
    return errorType;
  }

  public String getMessage() {
    return getError().getMessage();
  }

  @Override
  public String toString() {
    return "\nErrorType: " + errorType + "\nError: " + error;
  }

  public static final class RegionPeerNotElected {
    private static final String ERROR_MESSAGE = "Region Peer not elected. Please try later";
    private static final Pdpb.Error DEFAULT_ERROR =
        Pdpb.Error.newBuilder().setMessage(ERROR_MESSAGE).build();
    private static final ErrorType ERROR_TYPE = ErrorType.REGION_PEER_NOT_ELECTED;
    public static final Error DEFAULT_INSTANCE =
        Error.newBuilder(DEFAULT_ERROR).setErrorType(ERROR_TYPE).build();
  }

  public static final class Builder {
    private Pdpb.Error error_;
    private ErrorType errorType_ = ErrorType.PD_ERROR;

    public Builder() {}

    public Builder(Pdpb.Error error) {
      this.error_ = error;
    }

    public Builder setError(Pdpb.Error error) {
      this.error_ = error;
      return this;
    }

    public Builder setErrorType(ErrorType errorType) {
      this.errorType_ = errorType;
      return this;
    }

    public org.tikv.common.pd.Error build() {
      return new Error(error_, errorType_);
    }
  }
}
