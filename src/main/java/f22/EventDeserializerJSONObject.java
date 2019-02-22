package f22;

import java.io.UnsupportedEncodingException;

import org.apache.flink.api.common.serialization.AbstractDeserializationSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.json.JSONException;
import org.json.JSONObject;

public class EventDeserializerJSONObject extends AbstractDeserializationSchema<Tuple2<String, JSONObject>> {
    private static final long serialVersionUID = 1L;

    @Override
    public Tuple2<String, JSONObject> deserialize(byte[] message){
    	Tuple2<String, JSONObject> emptyTuple = Tuple2.of(null, new JSONObject("{}"));
    	if(message == null) {
			return emptyTuple;
		}
    	try {
			return Tuple2.of(null, new JSONObject(new String(message, "UTF-8")));
		} catch (JSONException e) {
			e.printStackTrace();
			return emptyTuple;
		} catch (UnsupportedEncodingException e) {
			e.printStackTrace();
			return emptyTuple;
		}
    }
}
