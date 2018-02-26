package com.zbiljic.apache.drill.exec.store.regex;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import javax.annotation.concurrent.Immutable;
import java.util.Objects;

import static java.util.Objects.requireNonNull;

@Immutable
public class Column {

  private final String name;
  private final String type;

  @JsonCreator
  public Column(@JsonProperty("name") String name,
                @JsonProperty("type") String type) {
    this.name = requireNonNull(name, "name is null");
    // default type is varchar
    this.type = type == null
      ? "VARCHAR"
      : type.toUpperCase();
  }

  @JsonProperty
  public String getName() {
    return name;
  }

  @JsonProperty
  public String getType() {
    return type;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    Column column = (Column) o;
    return Objects.equals(name, column.name) &&
      Objects.equals(type, column.type);
  }

  @Override
  public int hashCode() {
    return Objects.hash(name, type);
  }
}
