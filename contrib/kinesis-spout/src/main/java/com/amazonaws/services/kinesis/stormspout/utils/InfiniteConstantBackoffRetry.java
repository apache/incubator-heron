/*
 * Copyright 2013-2014 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Amazon Software License (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 * http://aws.amazon.com/asl/
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */

package com.amazonaws.services.kinesis.stormspout.utils;

import java.util.concurrent.Callable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Does an infinite constant time backoff against an exception.
 *
 * @param <T> return type
 */
public class InfiniteConstantBackoffRetry<T> implements Callable<T> {
    private static final Logger LOG = LoggerFactory.getLogger(InfiniteConstantBackoffRetry.class);

    private final long backoffMillis;
    private final Class<? extends Exception> retryOn;
    private final Callable<T> f;

    /** Constructor.
     * @param backoffMillis Backoff time
     * @param retryOn Exception we should retry on.
     * @param f Callable (function) we should call/retry
     */
    public InfiniteConstantBackoffRetry(final long backoffMillis,
            final Class<? extends Exception> retryOn,
            final Callable<T> f) {
        this.backoffMillis = backoffMillis;
        this.retryOn = retryOn;
        this.f = f;
    }

    @Override
    public T call() {
        try {
            return checkedCall();
        } catch (RuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private T checkedCall() throws Exception {
        while (true) {
            try {
                return f.call();
            } catch (Exception e) {
                if (retryOn.isAssignableFrom(e.getClass())) {
                    LOG.debug("Caught exception of type " + retryOn.getName() + ", backing off for " + backoffMillis
                            + " ms.");
                    Thread.sleep(backoffMillis);
                } else {
                    throw e;
                }
            }
        }
    }
}
