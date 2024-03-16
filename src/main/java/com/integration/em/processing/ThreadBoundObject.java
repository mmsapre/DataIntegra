package com.integration.em.processing;

import com.integration.em.utils.Function;

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class ThreadBoundObject<T> {

    private Map<Thread, T> objects = new ConcurrentHashMap<>();
    private Function<T, Thread> createObject;

    public ThreadBoundObject(Function<T, Thread> createObject) {
        this.createObject = createObject;
    }

    public T get() {
        Thread thisThread = Thread.currentThread();

        T theObject = objects.get(thisThread);

        if(theObject==null) {
            theObject = createObject.execute(thisThread);
            objects.put(thisThread, theObject);
        }

        return theObject;
    }

    public Collection<T> getAll() {
        return objects.values();
    }
}
