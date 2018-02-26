package com.zbiljic.apache.drill.exec.store.regex;

import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.common.logical.StoragePluginConfig;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.proto.UserBitShared;
import org.apache.drill.exec.server.DrillbitContext;
import org.apache.drill.exec.store.RecordReader;
import org.apache.drill.exec.store.RecordWriter;
import org.apache.drill.exec.store.dfs.DrillFileSystem;
import org.apache.drill.exec.store.dfs.easy.EasyFormatPlugin;
import org.apache.drill.exec.store.dfs.easy.EasyWriter;
import org.apache.drill.exec.store.dfs.easy.FileWork;
import org.apache.hadoop.conf.Configuration;

import java.io.IOException;
import java.util.List;

public class RegexFormatPlugin extends EasyFormatPlugin<RegexFormatPluginConfig> {

  @SuppressWarnings("unused")
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(RegexFormatPlugin.class);

  private static final boolean IS_READABLE = true;
  private static final boolean IS_WRITABLE = false;
  private static final boolean IS_BLOCK_SPLITTABLE = false;
  private static final boolean IS_COMPRESSIBLE = true;
  private static final String DEFAULT_NAME = "regex";

  private final RegexFormatPluginConfig config;

  public RegexFormatPlugin(String name,
                           DrillbitContext context,
                           Configuration fsConf,
                           StoragePluginConfig storageConfig) {
    this(name, context, fsConf, storageConfig, new RegexFormatPluginConfig());
  }

  public RegexFormatPlugin(String name,
                           DrillbitContext context,
                           Configuration fsConf,
                           StoragePluginConfig storageConfig,
                           RegexFormatPluginConfig formatConfig) {
    super(name, context, fsConf, storageConfig, formatConfig, IS_READABLE, IS_WRITABLE, IS_BLOCK_SPLITTABLE, IS_COMPRESSIBLE, formatConfig.getExtensions(), DEFAULT_NAME);
    this.config = formatConfig;
  }

  @Override
  public boolean supportsPushDown() {
    return true;
  }

  @Override
  public RecordReader getRecordReader(FragmentContext context, DrillFileSystem dfs, FileWork fileWork, List<SchemaPath> columns, String userName) throws ExecutionSetupException {
    return new RegexRecordReader(context, fileWork.getPath(), dfs, columns, config);
  }

  @Override
  public RecordWriter getRecordWriter(FragmentContext context, EasyWriter writer) throws IOException {
    return null;
  }

  @Override
  public int getReaderOperatorType() {
    return UserBitShared.CoreOperatorType.JSON_SUB_SCAN_VALUE;
  }

  @Override
  public int getWriterOperatorType() {
    throw new UnsupportedOperationException();
  }
}
