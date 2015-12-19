package backtype.storm.spout;

import java.io.Serializable;
import java.util.List;

import backtype.storm.tuple.Fields;

public interface Scheme extends Serializable {
    public List<Object> deserialize(byte[] ser);
    public Fields getOutputFields();
}
