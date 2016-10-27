package dp.hdfs.lines; /**
 * Copyright 2015 Confluent Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 **/

import com.datapipeline.base.exceptions.NeedOpsException;
import com.datapipeline.base.kafka.SinkRecordSerializer;

import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.io.DatumWriter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.kafka.connect.sink.SinkRecord;
import org.codehaus.jettison.json.JSONException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

import dp.hdfs.record.parse.RecordParse;
import io.confluent.connect.avro.AvroData;
import io.confluent.connect.hdfs.RecordWriter;
import io.confluent.connect.hdfs.RecordWriterProvider;

public class LineWriterProvider implements RecordWriterProvider {


    private static final Logger log = LoggerFactory.getLogger(LineWriterProvider.class);

    //TODO  get EXTENSION from json
    private final static String EXTENSION = ".tsv";


    @Override
    public String getExtension() {
        return EXTENSION;
    }

    @Override
    public RecordWriter<SinkRecord> getRecordWriter(
        Configuration conf, final String fileName, SinkRecord record, final AvroData avroData)
        throws IOException {
        DatumWriter<Object> datumWriter = new GenericDatumWriter<>();
        final DataFileWriter<Object> writer = new DataFileWriter<>(datumWriter);
        Path path = new Path(fileName);
        final FSDataOutputStream out = path.getFileSystem(conf).create(path);

        return new RecordWriter<SinkRecord>() {
            @Override
            public void write(SinkRecord record) throws IOException {
                try {
                    String line = RecordParse.getInsertUpdateDataLine(record);
                    out.write(line.getBytes());
                } catch (NeedOpsException e) {
                    e.printStackTrace();
                } catch (JSONException e) {
                    log.warn("[LineWriterProvider] a record can not parse");
                }
            }

            @Override
            public void close() throws IOException {
                writer.close();
                out.close();
            }
        };
    }
}
