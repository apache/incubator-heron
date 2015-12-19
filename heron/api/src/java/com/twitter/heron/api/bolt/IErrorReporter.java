package com.twitter.heron.api.bolt;

public interface IErrorReporter {
    void reportError(Throwable error);
}
