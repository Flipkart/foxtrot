/**
 * Copyright 2014 Flipkart Internet Pvt. Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.flipkart.foxtrot.core.table;

import com.flipkart.foxtrot.common.Table;
import com.flipkart.foxtrot.core.exception.FoxtrotException;
import com.yammer.dropwizard.lifecycle.Managed;

import java.util.List;

/**
 * User: Santanu Sinha (santanu.sinha@flipkart.com)
 * Date: 15/03/14
 * Time: 10:08 PM
 */
public interface TableMetadataManager extends Managed {

    void save(Table table) throws FoxtrotException;

    Table get(String tableName) throws FoxtrotException;

    List<Table> get() throws FoxtrotException;

    boolean exists(String tableName) throws FoxtrotException;

    void delete(String tableName) throws FoxtrotException;
}
