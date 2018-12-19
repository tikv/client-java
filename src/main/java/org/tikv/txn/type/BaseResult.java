package org.tikv.txn.type;

public class BaseResult {

    protected String errorMsg;

    public boolean hasError() {
        return errorMsg != null && !errorMsg.equals("");
    }

    public String getErrorMsg() {
        return errorMsg;
    }

    public void setErrorMsg(String errorMsg) {
        this.errorMsg = errorMsg;
    }
}
