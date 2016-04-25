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
package com.flipkart.foxtrot.server.console;

import com.flipkart.foxtrot.core.exception.FoxtrotException;

import java.util.List;

public interface ConsolePersistence {
    public void save(Console console) throws FoxtrotException;
    public Console get(final String id) throws FoxtrotException;
    public List<Console> get() throws FoxtrotException;
    public void delete(final String id) throws FoxtrotException;
}
