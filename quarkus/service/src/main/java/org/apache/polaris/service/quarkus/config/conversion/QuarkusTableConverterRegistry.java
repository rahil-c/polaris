/*
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

package org.apache.polaris.service.quarkus.config.conversion;

import io.smallrye.common.annotation.Identifier;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Instance;
import jakarta.inject.Inject;
import org.apache.polaris.service.conversion.TableConverter;
import org.apache.polaris.service.conversion.TableConverterRegistry;
import org.jboss.resteasy.reactive.common.util.CaseInsensitiveMap;

import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

@ApplicationScoped
public class QuarkusTableConverterRegistry implements TableConverterRegistry {

    private final CaseInsensitiveMap<TableConverter> converterMap = new CaseInsensitiveMap<>();

    @Inject
    public QuarkusTableConverterRegistry(
        QuarkusConverterConfig config,
        Instance<TableConverter> converters) {
        Map<String, TableConverter> beansById = converters.stream()
            .collect(Collectors.toMap(
                converter -> converter.getClass().getAnnotation(Identifier.class).value(),
                Function.identity()
            ));

        config.converters.forEach((key, identifier) -> {
            TableConverter converter = beansById.get(identifier);
            if (converter != null) {
                converterMap.put(key, List.of(converter));
            }
        });
    }

    /** Load the TableConverter for a format, case-insensitive */
    public TableConverter getConverter(String format) {
        return converterMap.getFirst(format);
    }
}
