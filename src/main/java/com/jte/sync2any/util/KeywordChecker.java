/*
 *  Copyright 1999-2019 Seata.io Group.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package com.jte.sync2any.util;

/**
 * The interface Keyword checker.
 *
 * @author Wu
 */
public interface KeywordChecker {
    /**
     * check whether given field name and table name use keywords
     *
     * @param fieldOrTableName the field or table name
     * @return boolean
     */
    boolean check(String fieldOrTableName);


    /**
     * check whether given field or table name use keywords. the method has database special logic.
     * @param fieldOrTableName
     * @return true: need to escape. false: no need to escape.
     */
    boolean checkEscape(String fieldOrTableName);

}
