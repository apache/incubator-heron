package org.apache.storm.serialization.types;

import java.util.ArrayList;
import java.util.Collection;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.serializers.CollectionSerializer;


public class ArrayListSerializer extends CollectionSerializer {
    @Override
    public Collection create(Kryo kryo, Input input, Class<Collection> type) {
        return new ArrayList();
    }    
}
