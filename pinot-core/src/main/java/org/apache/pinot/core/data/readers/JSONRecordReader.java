/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pinot.core.data.readers;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Map;
import org.apache.pinot.common.data.DimensionFieldSpec;
import org.apache.pinot.common.data.FieldSpec;
import org.apache.pinot.common.data.Schema;
import org.apache.pinot.common.data.TimeFieldSpec;
import org.apache.pinot.common.data.TimeGranularitySpec;
import org.apache.pinot.common.utils.JsonUtils;
import org.apache.pinot.core.data.GenericRow;


/**
 * Record reader for JSON file.
 */
public class JSONRecordReader implements RecordReader {
  private final JsonFactory _factory = new JsonFactory();
  private final File _dataFile;
  private final Schema _schema;

  private JsonParser _parser;
  private Iterator<Map> _iterator;

  public JSONRecordReader(File dataFile, Schema schema)
      throws IOException {
    _dataFile = dataFile;
    _schema = schema;

    init();
  }

  private void init()
      throws IOException {
    _parser = _factory.createParser(RecordReaderUtils.getFileReader(_dataFile));
    try {
      _iterator = JsonUtils.DEFAULT_MAPPER.readValues(_parser, Map.class);
    } catch (Exception e) {
      _parser.close();
      throw e;
    }
  }

  @Override
  public boolean hasNext() {
    return _iterator.hasNext();
  }

  @Override
  public GenericRow next() {
    return next(new GenericRow());
  }

  @Override
  public GenericRow next(GenericRow reuse) {
    Map record = _iterator.next();

    for (FieldSpec fieldSpec : _schema.getAllFieldSpecs()) {
      if (fieldSpec.getFieldType() == FieldSpec.FieldType.TIME) {
        // For TIME field spec, if incoming and outgoing time column name are the same, use incoming field spec to fill
        // the row; otherwise, fill both incoming and outgoing time column
        TimeFieldSpec timeFieldSpec = (TimeFieldSpec) fieldSpec;
        String incomingTimeColumnName = timeFieldSpec.getIncomingTimeColumnName();
        Object incomingTimeJsonValue = record.get(incomingTimeColumnName);
        if (incomingTimeJsonValue != null) {
          // Treat incoming time column as a single-valued dimension column
          TimeGranularitySpec incomingGranularitySpec = timeFieldSpec.getIncomingGranularitySpec();
          reuse.putField(incomingTimeColumnName, RecordReaderUtils.convertToDataType(incomingTimeJsonValue.toString(),
              new DimensionFieldSpec(incomingTimeColumnName, incomingGranularitySpec.getDataType(), true)));
        }
        // Fill outgoing time column if its name is not the same as incoming time column
        String outgoingTimeColumnName = timeFieldSpec.getOutgoingTimeColumnName();
        if (!outgoingTimeColumnName.equals(incomingTimeColumnName)) {
          Object outgoingTimeJsonValue = record.get(outgoingTimeColumnName);
          if (outgoingTimeJsonValue != null) {
            reuse.putField(outgoingTimeColumnName,
                RecordReaderUtils.convertToDataType(outgoingTimeJsonValue.toString(), timeFieldSpec));
          }
        }
      } else {
        String fieldName = fieldSpec.getName();
        Object jsonValue = record.get(fieldName);
        if (fieldSpec.isSingleValueField()) {
          String token = jsonValue != null ? jsonValue.toString() : null;
          reuse.putField(fieldName, RecordReaderUtils.convertToDataType(token, fieldSpec));
        } else {
          reuse.putField(fieldName, RecordReaderUtils.convertToDataTypeArray((ArrayList) jsonValue, fieldSpec));
        }
      }
    }

    return reuse;
  }

  @Override
  public void rewind()
      throws IOException {
    _parser.close();
    init();
  }

  @Override
  public Schema getSchema() {
    return _schema;
  }

  @Override
  public void close()
      throws IOException {
    _parser.close();
  }
}
