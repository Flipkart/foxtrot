/**
 * Copyright 2014 Flipkart Internet Pvt. Ltd.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */
package com.flipkart.foxtrot.server.console;

import java.util.List;

public interface ConsolePersistence {

    void save(Console console);

    Console get(final String id);

    List<Console> get();

    void delete(final String id);

    void saveV2(ConsoleV2 console,
                boolean freshConsole);

    ConsoleV2 getV2(final String id);

    List<ConsoleV2> getV2();

    void deleteV2(final String id);

    List<ConsoleV2> getAllOldVersions(final String name,
                                      final String sortBy);

    ConsoleV2 getOldVersion(final String id);

    void deleteOldVersion(String id);

    void setOldVersionAsCurrent(final String id);
}
