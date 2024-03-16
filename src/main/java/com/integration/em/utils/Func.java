package com.integration.em.utils;

public interface Func<TOut, TIn> {

    TOut invoke(TIn in);
}
