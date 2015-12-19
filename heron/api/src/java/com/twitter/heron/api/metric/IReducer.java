package com.twitter.heron.api.metric;

public interface IReducer<T> {
    T init();
    T reduce(T accumulator, Object input);
    Object extractResult(T accumulator);
}
