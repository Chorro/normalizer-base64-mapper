package com.github.chorro.normalizer.func;

import io.wizzie.metrics.MetricsManager;
import io.wizzie.normalizer.funcs.MapperFunction;
import org.apache.kafka.streams.KeyValue;

import java.util.*;

import static com.cookingfox.guava_preconditions.Preconditions.checkNotNull;

public class Base64Mapper extends MapperFunction {

  private List<String> fields = new ArrayList<>();

  public void prepare(Map<String, Object> properties, MetricsManager metricsManager) {
    fields = checkNotNull((List<String>) properties.get("fields"), "fields cannot be null");
  }

  public KeyValue<String, Map<String, Object>> process(String key, Map<String, Object> value) {
    fields.forEach((field) -> {
      if (value.containsKey(field)) {

        Object encodedField = value.get(field);

        if (encodedField instanceof String) {

          String decodedField = new String(Base64.getDecoder().decode((String) encodedField));
          value.put(field, decodedField);
        }
      }
    });

    return new KeyValue<>(key, value);
  }

  public void stop() { }
}
