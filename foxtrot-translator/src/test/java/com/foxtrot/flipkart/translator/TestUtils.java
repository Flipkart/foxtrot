/**
 * Copyright 2014 Flipkart Internet Pvt. Ltd.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.foxtrot.flipkart.translator;

import com.foxtrot.flipkart.translator.config.TranslatorConfig;
import com.jayway.jsonpath.Configuration;
import com.jayway.jsonpath.Option;
import com.jayway.jsonpath.spi.json.JacksonJsonNodeJsonProvider;
import com.jayway.jsonpath.spi.json.JsonProvider;
import com.jayway.jsonpath.spi.mapper.JacksonMappingProvider;
import com.jayway.jsonpath.spi.mapper.MappingProvider;

import java.util.EnumSet;
import java.util.Set;

/**
 * Created by rishabh.goyal on 28/04/14.
 */
public class TestUtils {


    public static TranslatorConfig createTranslatorConfigWithRawKeyV1() {
        TranslatorConfig translatorConfig = new TranslatorConfig();
        translatorConfig.setRawKeyVersion("1.0");
        return translatorConfig;
    }

    public static TranslatorConfig createTranslatorConfigWithRawKeyV2() {
        TranslatorConfig translatorConfig = new TranslatorConfig();
        translatorConfig.setRawKeyVersion("2.0");
        return translatorConfig;
    }

    public static TranslatorConfig createTranslatorConfigWithRawKeyV3() {
        TranslatorConfig translatorConfig = new TranslatorConfig();
        translatorConfig.setRawKeyVersion("3.0");
        return translatorConfig;
    }

    public static void setupJsonPath() {
        Configuration.setDefaults(new Configuration.Defaults() {
            @Override
            public JsonProvider jsonProvider() {
                return new JacksonJsonNodeJsonProvider();
            }

            @Override
            public Set<Option> options() {
                return EnumSet.noneOf(Option.class);
            }

            @Override
            public MappingProvider mappingProvider() {
                return new JacksonMappingProvider();
            }
        });
    }

}