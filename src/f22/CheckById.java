package f22;

import org.apache.flink.api.common.functions.RichMapFunction;
import java.util.List;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;

public class CheckById extends RichMapFunction<String, List<String>> {
	private static final long serialVersionUID = 1L;
	private ListState<String> sum;

	@Override
    public List<String> map(String input) throws Exception {
    	sum.add(input);
    	return (List<String>) sum.get();
    }

	public void open(Configuration cfg) {
		ListStateDescriptor<String> listState = new ListStateDescriptor<String>("state", String.class);
		System.out.println(listState);
        sum = getRuntimeContext().getListState(listState);
    }
}
