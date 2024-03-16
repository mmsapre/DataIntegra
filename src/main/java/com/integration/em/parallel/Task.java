package com.integration.em.parallel;

public abstract class Task implements Parallel.ITask {

    private Object userData;
    public Object getUserData() {
        return userData;
    }
    public void setUserData(Object userData) {
        this.userData = userData;
    }
    public abstract void execute();
}
