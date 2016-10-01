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

package com.amazonaws.services.kinesis.stormspout.state.zookeeper;

import java.util.Arrays;

import com.amazonaws.services.kinesis.stormspout.state.zookeeper.NodeFunction.Mod;
import com.google.common.base.Function;

/**
 * Transformation function for a node (e.g. shard state in Zookeeper). Calls initialize() if the node is not created,
 * apply(oldNodeValue) otherwise.
 *
 * Based on oldNodeValue, the function can determine whether to update the node (Mod.modification)
 * or leave it as is (Mod.noModification).
 */
abstract class NodeFunction implements Function<byte[], Mod<byte[]>> {
    @Override
    public abstract Mod<byte[]> apply(byte[] x);

    /**
     * @return value to initialize an empty node with.
     */
    public abstract byte[] initialize();

    /**
     * Represents a potential modification of some storage of type T. If hasModification(),
     * value in storage should be updated with return value of get(). Otherwise, should stay
     * the same.
     *
     * @param <T> type of elements in storage.
     */
    public static class Mod<T> {
        private final boolean hasModification;
        private final T value;

        private Mod(final T value, final boolean hasModification) {
            this.value = value;
            this.hasModification = hasModification;
        }

        /**
         * Pre : there is a modification.
         * @return the modification.
         */
        public T get() {
            assert hasModification;
            return value;
        }

        /**
         * @return true is this Mod instance is a modification, false otherwise.
         */
        public boolean hasModification() {
            return hasModification;
        }

        /**
         * Create a Mod instance that represents "no modification" to be made to a node.
         *
         * @return a no modification Mod instance.
         * @param <T>  type of object in store.
         */
        public static <T> Mod<T> noModification() {
            return new Mod<T>(null, false);
        }

        /**
         * Create a Mod instance that represents a modification to be made to a node.
         *
         * @param value  the value that will replace the current one in the node.
         * @return a modification Mod instance with a value.
         * @param <T>  type of object in store.
         */
        public static <T> Mod<T> modification(final T value) {
            return new Mod<T>(value, true);
        }
    }

    // Helpers
    /**
     * @param val  constant value that will be returned.
     * @return a NodeFunction that always returns val.
     */
    static NodeFunction constant(final byte[] val) {
        return new NodeFunction() {
            @Override
            public Mod<byte[]> apply(byte[] discarded) {
                if (Arrays.equals(val, discarded)) {
                    return Mod.noModification();
                } else {
                    return Mod.modification(val);
                }
            }

            @Override
            public byte[] initialize() {
                return val;
            }
        };
    }
}
