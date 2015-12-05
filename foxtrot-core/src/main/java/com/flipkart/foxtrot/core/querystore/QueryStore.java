/**
 * Copyright 2014 Flipkart Internet Pvt. Ltd.
 * <p/>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.flipkart.foxtrot.core.querystore;

import com.flipkart.foxtrot.common.Document;
import com.flipkart.foxtrot.common.TableFieldMapping;
import com.flipkart.foxtrot.core.common.Identifiable;

import java.util.List;
import java.util.Set;

/**
 * User: Santanu Sinha (santanu.sinha@flipkart.com)
 * Date: 12/03/14
 * Time: 9:25 PM
 */
public interface QueryStore extends Identifiable {

    void init(final String table) throws QueryStoreException;

    void save(final String table, final Document document) throws QueryStoreException;

    void save(final String table, final List<Document> documents) throws QueryStoreException;

    Document get(final String table, final String id) throws QueryStoreException;

    List<Document> get(final String table, final List<String> ids) throws QueryStoreException;

    TableFieldMapping getFieldMappings(final String table) throws QueryStoreException;

    void cleanupAll() throws QueryStoreException;

    void cleanup(final String table) throws QueryStoreException;

    void cleanup(final Set<String> tables) throws QueryStoreException;
}
