package com.twitter.heron.api.metric;

public interface ICombiner<T> {
    public T identity();
    public T combine(T a, T b);
}
