//  Copyright 2017 Twitter. All rights reserved.
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//  http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class StateExample {

  interface TypedState<K extends Serializable, V extends Serializable> {
    void put(K key, V value);
    V get(K key);
    Set<K> keySet();
  }

  interface ITypedStatefulComponent<K extends Serializable, V extends Serializable> {
    void initState(TypedState<K, V> state);
  }

  static class MyStateImpl implements TypedState {
    private Map<Serializable, Serializable> map = new HashMap<>();

    @Override
    public void put(Serializable key, Serializable value) {
      map.put(key, value);
    }

    @Override
    public Serializable get(Serializable key) {
      return map.get(key);
    }

    @Override
    public Set keySet() {
      return map.keySet();
    }
  }

  static class MyStatefulBolt implements ITypedStatefulComponent<String, Integer> {
    @Override
    public void initState(TypedState<String, Integer> state) {
      for (String key : state.keySet()) {
        System.out.printf("key: %s, value %d", key, state.get(key));
      }
    }
  }

  static TypedState loadStateFromDisk() {
    Serializable key = "String";
    Serializable value = 2;

    TypedState state = new MyStateImpl();
    state.put(key, value);
    return state;
  }

  public static void main(String[] args) {
    MyStatefulBolt bolt = new MyStatefulBolt();
    bolt.initState(loadStateFromDisk());
  }
}

