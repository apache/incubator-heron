package org.apache.storm.task;

public interface IErrorReporter {
    void reportError(Throwable error);
}
