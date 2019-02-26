package f22;

import java.io.UnsupportedEncodingException;

import org.apache.flink.api.common.serialization.AbstractDeserializationSchema;
import org.apache.flink.api.java.tuple.Tuple2;

public class MyDeserializationSchema extends AbstractDeserializationSchema<Tuple2<String, String>> {
	private static final long serialVersionUID = 1L;

	@Override
    public Tuple2<String, String> deserialize(byte[] message){
		Tuple2<String, String> emptyTuple = Tuple2.of(null, "");
		if(message == null) {
			return emptyTuple;
		}
        try {
			return Tuple2.of(null, new String(message, "UTF-8"));
		} catch (UnsupportedEncodingException e) {
			e.printStackTrace();
			return emptyTuple;
		}
    }
}
