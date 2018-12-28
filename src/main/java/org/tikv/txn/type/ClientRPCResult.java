package org.tikv.txn.type;

public class ClientRPCResult {
    boolean success;
    boolean retry;
    String error;

    public ClientRPCResult(boolean success, boolean retry, String error) {
        this.success = success;
        this.retry = retry;
        this.error = error;
    }

    public boolean isSuccess() {
        return success;
    }

    public void setSuccess(boolean success) {
        this.success = success;
    }

    public boolean isRetry() {
        return retry;
    }

    public void setRetry(boolean retry) {
        this.retry = retry;
    }

    public String getError() {
        return error;
    }

    public void setError(String error) {
        this.error = error;
    }
}
