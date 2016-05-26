// Copyright 2016 Twitter. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.twitter.heron.storage;

import java.util.Map;

/**
 * Abstract class implemented by all the storage layers of Storm. Provides abstraction to bolts and
 * spout to store arbitrary Key -> Value data in ay underlying storage, per component. This API
 * provides a 2 dimensional map. pKey is like rows of a table and lKey is columns. Some storage
 * system like HBase provide a time dimension to data, storm storage doesn't need it
 * and only use the latest value for a key.
 * All implementation of MetadataStore should strive to be interoperable i.e. Clients can read
 * data written by other MetadataStore.
 */
@SuppressWarnings("rawtypes")
public abstract class MetadataStore {
  protected String uId;
  protected String topologyName;
  protected String componentId;
  public StoreSerializer serializer;
  protected String keyPrefix;

  /** Checks if initialization has been done and has succeeded.
   */
  public abstract boolean isInitialized();

  /** Adds partition key. Value can be null to create a placeholder partition.
   * It is advised to have pkey topologyName_uId_componentId_keySuffix. set is supposed to be
   * Idempotent. If key already exist, it will be reset.
   */
  public abstract <T> boolean setPKey(String keySuffix, T value);

  /** Add a local Key - Value to a pkey. PKey and LKey are separated to have API
   * which provide separation between logical keys and partition keys.
   */
  public abstract <T> boolean setLKey(String pKey, String lKey, T value);

  /**
   * Reads the data associated with PKey. If the PKey also has lkey they will not be read.
   * If the key is not present null will be returned. If the key is present but doesn't contain any
   * data, an empty byte array is returned.
   */
  public abstract byte[] readPKey(String keySuffix);

  /** Reads the data for a LKey. */
  public abstract byte[] readLKey(String pKey, String lKey);

  /** Read all lKey for a pKey. Returns a Map of key -> object */
  public abstract Map<String, byte[]> readAllLKey(String pKey);

  /** Closes of datastore connecttion if any */
  public abstract void close();

  /** Creates Key from keySuffix */
  public String getPKey(String keySuffix) {
    return String.format("%s-%s", keyPrefix, keySuffix.trim());
  }

  public String getPKeySuffix(String key) {
    return key.substring(keyPrefix.length());
  }

  /** Called to initialize a Datastore. Implementing classes can override this to initialize
   * there specific storage layer.
   */
  public boolean initialize(
      String paramUId, String paramTopologyName, String paramComponentId,
      StoreSerializer paramSerializer) {
    uId = paramUId;
    topologyName = paramTopologyName;
    componentId = paramComponentId;
    keyPrefix = String.format("%s_%s_%s", topologyName, uId, componentId);
    System.out.println("@@@@ " + keyPrefix);
    keyPrefix = keyPrefix.replace(" ", "");
    serializer = paramSerializer != null ? paramSerializer
        : new StoreSerializer.DefaultSerializer();
    return true;
  }
}
