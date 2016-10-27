package dp.hdfs.sinkparse;

import com.google.common.base.Joiner;

import com.datapipeline.base.exceptions.NeedOpsException;
import com.datapipeline.base.kafka.DatatestSinkRecord;
import com.datapipeline.base.kafka.SinkRecordSerializer;

import org.apache.kafka.connect.data.Struct;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * Created by mx on 10/27/16.
 */
public class SinkParseTest {

    Struct value;
    JSONObject expected;


    @Before
    public void setUp() throws JSONException {
        value = (new DatatestSinkRecord()).complexStruct;
        expected = new JSONObject("{\"before\":{\"columnA\":true},\"after\":{\"columnA\":true,\"columnB\":32},\"ts\":42}");
    }

    @Test
    public void testParse() {

        try {
            JSONObject x = SinkRecordSerializer.parseSinkRecordStruct(value);
            System.out.println(x.get("after").toString());
            JSONObject afterValue = x.getJSONObject("after");

            Iterator iterator = afterValue.keys();
            String key = null;
            List<String> list = new ArrayList<>();

//          afterValue.keys().forEachRemaining(key -> {
//              try {
//                  System.out.println(afterValue.get(key.toString()));
//              } catch (JSONException e) {
//                  e.printStackTrace();
//              }
//            });


            while (iterator.hasNext()) {
                key = iterator.next().toString();
                list.add(afterValue.get(key).toString());
            }

            String line = Joiner.on(",").join(list);

            System.out.println(line);

            assert line.equals("true,32");


        } catch (NeedOpsException e) {
            e.printStackTrace();
        } catch (JSONException e) {
            e.printStackTrace();
        }

    }

}
