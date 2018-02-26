package com.zbiljic.apache.drill.exec.store.regex;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.google.common.collect.ImmutableList;
import org.apache.drill.common.logical.FormatPluginConfig;

import java.util.List;
import java.util.Objects;

@JsonTypeName("regex")
@JsonInclude(Include.NON_DEFAULT)
public class RegexFormatPluginConfig implements FormatPluginConfig {

  public List<String> extensions;
  public List<Column> columns;
  public String pattern;
  public Boolean errorOnMismatch = false;

  @JsonInclude(JsonInclude.Include.NON_DEFAULT)
  public List<String> getExtensions() {
    if (extensions == null) {
      return ImmutableList.of();
    }
    return extensions;
  }

  public List<Column> getColumns() {
    return columns;
  }

  public String getPattern() {
    return pattern;
  }

  public Boolean getErrorOnMismatch() {
    return errorOnMismatch;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    RegexFormatPluginConfig that = (RegexFormatPluginConfig) o;
    return Objects.equals(columns, that.columns) &&
      Objects.equals(pattern, that.pattern) &&
      Objects.equals(errorOnMismatch, that.errorOnMismatch);
  }

  @Override
  public int hashCode() {
    return Objects.hash(columns, pattern, errorOnMismatch);
  }
}
