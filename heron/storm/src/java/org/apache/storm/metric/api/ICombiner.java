package org.apache.storm.metric.api;

public interface ICombiner<T> {
    public T identity();
    public T combine(T a, T b);
}
