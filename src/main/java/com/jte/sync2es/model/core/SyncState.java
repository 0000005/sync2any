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
package com.jte.sync2es.model.core;

/**
 * sync state
 *
 * @author sharajava
 */
public enum SyncState {
    /**
     * waiting to start
     */
    WAITING(0),
    /**
     * sync is stopped
     */
    STOPPED(1),
    /**
     * loading origin data
     */
    LOADING_ORIGIN_DATA(2),
    /**
     * sync is running
     */
    SYNCING(3);

    private int i;

    SyncState(int i) {
        this.i = i;
    }

    /**
     * Value int.
     *
     * @return the int
     */
    public int value() {
        return this.i;
    }

    /**
     * Value of index type.
     *
     * @param i the
     * @return the index type
     */
    public static SyncState valueOf(int i) {
        for (SyncState t : values()) {
            if (t.value() == i) {
                return t;
            }
        }
        throw new IllegalArgumentException("Invalid SyncState:" + i);
    }
}