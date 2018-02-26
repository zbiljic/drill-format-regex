package com.zbiljic.apache.drill.exec.store.regex;

import com.google.common.base.Stopwatch;
import io.netty.buffer.DrillBuf;
import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.common.exceptions.UserException;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.ops.OperatorContext;
import org.apache.drill.exec.physical.impl.OutputMutator;
import org.apache.drill.exec.store.AbstractRecordReader;
import org.apache.drill.exec.store.dfs.DrillFileSystem;
import org.apache.drill.exec.vector.BaseValueVector;
import org.apache.drill.exec.vector.complex.impl.VectorContainerWriter;
import org.apache.drill.exec.vector.complex.writer.BaseWriter;
import org.apache.hadoop.fs.Path;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.ISODateTimeFormat;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.text.ParseException;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class RegexRecordReader extends AbstractRecordReader {

  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(RegexRecordReader.class);

  private final String inputPath;
  private final RegexFormatPluginConfig config;
  private final DrillFileSystem dfs;

  private DrillBuf buffer;

  private OperatorContext operatorContext;
  private VectorContainerWriter vectorWriter;
  private BufferedReader reader;

  private Pattern pattern;
  private int lineCount;
  private List<Column> columns;
  private boolean errorOnMismatch;

  public RegexRecordReader(FragmentContext context,
                           String inputPath,
                           DrillFileSystem dfs,
                           List<SchemaPath> columns,
                           RegexFormatPluginConfig config) {
    this.inputPath = inputPath;
    this.config = config;
    this.dfs = dfs;

    this.buffer = context.getManagedBuffer(4096);

    setColumns(columns);
  }

  @Override
  public void setup(OperatorContext context, OutputMutator output) throws ExecutionSetupException {
    this.operatorContext = context;
    this.vectorWriter = new VectorContainerWriter(output);

    try {
      InputStream fsStream = dfs.openPossiblyCompressedStream(new Path(inputPath));
      this.reader = new BufferedReader(new InputStreamReader(fsStream, "UTF-8"));
    } catch (IOException e) {
      logger.debug("Regex Reader Plugin setup: " + e.getMessage());
      throw new ExecutionSetupException(e);
    }

    String regex = config.getPattern();

    if (regex.isEmpty()) {
      throw UserException.parseError()
        .message("Regex parser requires a valid, non-empty regex in the plugin configuration")
        .build(logger);
    }

    pattern = Pattern.compile(regex);
    lineCount = 0;
    columns = config.getColumns();
    errorOnMismatch = config.getErrorOnMismatch();
  }

  @Override
  public int next() {
    Stopwatch watch = Stopwatch.createUnstarted();
    watch.start();

    vectorWriter.allocate();
    vectorWriter.reset();

    int recordCount = 0;

    try {
      BaseWriter.MapWriter map = vectorWriter.rootAsMap();
      String line = null;

      while (recordCount < BaseValueVector.INITIAL_VALUE_ALLOCATION && (line = this.reader.readLine()) != null) {
        lineCount++;

        // Skip empty lines
        if (line.trim().length() == 0) {
          continue;
        }

        vectorWriter.setPosition(recordCount);
        map.start();

        Matcher m = pattern.matcher(line);

        if (m.groupCount() == 0) {
          throw new ParseException(
            "Invalid Regular Expression: No Capturing Groups",
            0
          );
        } else if (m.groupCount() != (columns.size())) {
          throw new ParseException(
            "Invalid Regular Expression: Field names do not match capturing groups.  There are "
              + m.groupCount()
              + " captured groups in the data and "
              + columns.size()
              + " specified in the configuration.",
            0
          );
        }

        if (m.find()) {

          writeToMap(map, m);

        } else {
          if (errorOnMismatch) {
            throw new ParseException("Line does not match pattern: " + inputPath + "\n" + lineCount + ":\n" + line, 0);
          } else {
            String fieldName = "unmatched_lines";
            byte[] bytes = line.getBytes("UTF-8");
            this.buffer.setBytes(0, bytes, 0, bytes.length);
            map.varChar(fieldName).writeVarChar(0, bytes.length, buffer);
          }
        }

        map.end();
        recordCount++;
      }

      vectorWriter.setValueCount(recordCount);
      logger.debug("Took {} ms to get {} records", watch.elapsed(TimeUnit.MILLISECONDS), recordCount);
      return recordCount;

    } catch (Exception e) {
      throw UserException.dataReadError(e).build(logger);
    }
  }

  private void writeToMap(BaseWriter.MapWriter map, Matcher m) throws Exception {
    for (int i = 1; i <= m.groupCount(); i++) {

      Column column = columns.get(i - 1);
      String fieldName = column.getName();
      String type = column.getType();
      String fieldValue;

      fieldValue = m.group(i);

      if (fieldValue == null) {
        fieldValue = "";
      }

      switch (type) {
        case "BOOLEAN":
          writeBoolean(map, fieldName, fieldValue);
          break;
        case "TINYINT":
          writeByte(map, fieldName, fieldValue);
          break;
        case "SMALLINT":
          writeShort(map, fieldName, fieldValue);
          break;
        case "INT":
        case "INTEGER":
          writeInt(map, fieldName, fieldValue);
          break;
        case "BIGINT":
          writeLong(map, fieldName, fieldValue);
          break;
        case "FLOAT":
        case "FLOAT4":
          writeFloat(map, fieldName, fieldValue);
          break;
        case "DOUBLE":
        case "FLOAT8":
          writeDouble(map, fieldName, fieldValue);
          break;
        case "DATE":
          writeDate(map, fieldName, fieldValue);
          break;
        case "TIME":
          writeTime(map, fieldName, fieldValue);
          break;
        case "TIMESTAMP":
          writeTimeStamp(map, fieldName, fieldValue);
          break;
        case "VAR16CHAR":
          writeString16(map, fieldName, fieldValue);
          break;
        case "VARCHAR":
        default:
          writeString(map, fieldName, fieldValue);
          break;
      }

    }
  }

  private void writeBoolean(BaseWriter.MapWriter map, String fieldName, String fieldValue) {
    map.bit(fieldName).writeBit(Boolean.parseBoolean(fieldValue) ? 1 : 0);
  }

  private void writeByte(BaseWriter.MapWriter map, String fieldName, String fieldValue) {
    map.tinyInt(fieldName).writeTinyInt(Byte.parseByte(fieldValue));
  }

  private void writeShort(BaseWriter.MapWriter map, String fieldName, String fieldValue) {
    map.smallInt(fieldName).writeSmallInt(Short.parseShort(fieldValue));
  }

  private void writeInt(BaseWriter.MapWriter map, String fieldName, String fieldValue) {
    map.integer(fieldName).writeInt(Integer.parseInt(fieldValue));
  }

  private void writeLong(BaseWriter.MapWriter map, String fieldName, String fieldValue) {
    map.bigInt(fieldName).writeBigInt(Long.parseLong(fieldValue));
  }

  private void writeFloat(BaseWriter.MapWriter map, String fieldName, String fieldValue) {
    map.float4(fieldName).writeFloat4(Float.parseFloat(fieldValue));
  }

  private void writeDouble(BaseWriter.MapWriter map, String fieldName, String fieldValue) {
    map.float8(fieldName).writeFloat8(Double.parseDouble(fieldValue));
  }

  private void writeDate(BaseWriter.MapWriter map, String fieldName, String fieldValue) {
    DateTimeFormatter f = ISODateTimeFormat.date();
    DateTime date = DateTime.parse(fieldValue, f);
    map.date(fieldName).writeDate(date.getMillis());
  }

  private void writeTime(BaseWriter.MapWriter map, String fieldName, String fieldValue) {
    DateTimeFormatter f = ISODateTimeFormat.time();
    DateTime time = DateTime.parse(fieldValue, f);
    map.time(fieldName).writeTime(time.getMillisOfDay());
  }

  private void writeTimeStamp(BaseWriter.MapWriter map, String fieldName, String fieldValue) {
    DateTimeFormatter f = ISODateTimeFormat.dateTime();
    DateTime dateTime = DateTime.parse(fieldValue, f);
    map.timeStamp(fieldName).writeTimeStamp(dateTime.getMillis());
  }

  private void writeString(BaseWriter.MapWriter map, String fieldName, String fieldValue) {
    final byte[] strBytes = fieldValue.getBytes(StandardCharsets.UTF_8);
    buffer = buffer.reallocIfNeeded(strBytes.length);
    buffer.setBytes(0, strBytes);
    map.varChar(fieldName).writeVarChar(0, strBytes.length, buffer);
  }

  private void writeString16(BaseWriter.MapWriter map, String fieldName, String fieldValue) {
    final byte[] strBytes = fieldValue.getBytes(StandardCharsets.UTF_16);
    buffer = buffer.reallocIfNeeded(strBytes.length);
    buffer.setBytes(0, strBytes);
    map.varChar(fieldName).writeVarChar(0, strBytes.length, buffer);
  }

  @Override
  public void close() throws Exception {
    if (reader != null) {
      reader.close();
    }
  }
}
