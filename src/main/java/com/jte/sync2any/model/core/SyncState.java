/*

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
package com.jte.sync2any.model.core;

/**
 * sync state
 *
 * @author sharajava
 */
public enum SyncState {
    /**
     * init state
     */
    INACTIVE(0,"初始状态，未激活"),

    /**
     * loading origin data
     */
    LOADING_ORIGIN_DATA(1,"正在载入原始数据"),
    /**
     * waiting to start listening
     */
    WAIT_TO_LISTENING(2,"等待监听队列"),
    /**
     * sync is running
     */
    SYNCING(3,"同步中"),
    /**
     * sync is stopped
     */
    STOPPED(4,"已停止");

    private int i;
    private String desc;

    SyncState(int i,String desc) {
        this.desc=desc;
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
     * Value desc.
     *
     * @return the desc
     */
    public String desc() {
        return this.desc;
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