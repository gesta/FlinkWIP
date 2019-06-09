package f22;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.co.CoFlatMapFunction;
import org.apache.flink.util.Collector;
import org.json.JSONObject;

public class CheckEventIds implements CoFlatMapFunction<Tuple2<String,String>, Tuple2<String,JSONObject>, String> {
    private static final long serialVersionUID = 1L;
    private static List<String> ids = new ArrayList<>();
    private static String control = "";

    @Override
    public void flatMap1(Tuple2<String, String> input, Collector<String> col1){
        String message = input.f1;
        if (message.equals("start")){
            // Pause processing event
            control = "pause";
            return;
        } else if (message.equals("stop")) {
            // Start processing event
            control = "processing";
            return;
        } else if (message.equals("flush")) {
        	// Flush the collected SKUs
        	ids = new ArrayList<>();
        	return;
        }

        if (control.equals("pause")) {
            ids.add(message);
        }
    }

    @Override
    public void flatMap2(Tuple2<String,JSONObject> event, Collector<String> col2){
    	JSONObject offer = event.f1;
    	// Perform id inclusion check if in "processing" mode
        if (control.equals("processing") && offer.has("sku") && ids.contains(offer.get("sku").toString())) {
            col2.collect(offer.toString());
        }
        // Do not perform id inclusion check if there are no ids to compare to
        else if (ids.equals(Collections.emptyList())) {
            col2.collect(offer.toString());
        }
    }
}
