package com.integration.em.parallel;

public abstract class ExtendedRunnable implements Runnable{

    private Exception exception;
    public Exception getException() {
        return exception;
    }
    public void setException(Exception exception) {
        this.exception = exception;
    }
}
