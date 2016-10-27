package dp.hdfs.record.parse;

import com.google.common.base.Joiner;

import com.datapipeline.base.exceptions.NeedOpsException;
import com.datapipeline.base.kafka.SinkRecordSerializer;

import org.apache.kafka.connect.sink.SinkRecord;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * SinkRecord is avro styp.
 * parse avro to data
 */
public class RecordParse {


    private static final SinkRecordSerializer serializeRecord = new SinkRecordSerializer();

    public static String getInsertUpdateDataLine(SinkRecord record) throws NeedOpsException, JSONException {

        JSONObject parseJson = serializeRecord.serializeRecord(record);
        JSONObject afterValue = parseJson.getJSONObject("after");

        List<String> list = new ArrayList<>();

        Iterator iterator = afterValue.keys();
        String key = null;
        while (iterator.hasNext()) {
            key = iterator.next().toString();
            list.add(afterValue.get(key).toString());
        }

//          afterValue.keys().forEachRemaining(key -> {
//              try {
//                  System.out.println(afterValue.get(key.toString()));
//              } catch (JSONException e) {
//                  e.printStackTrace();
//              }
//            });
        return Joiner.on(",").join(list);
    }
}
