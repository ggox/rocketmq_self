/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.rocketmq.client.latency;

/**
 * 容错策略接口
 *
 * tips: 该接口目前只有一个应用的地方就是broker的容错，但是其实可用用于别的场景，是个通用的抽象接口规范，所以使用了泛型，方法参数也是用较为通用的name
 *
 * @param <T>
 */
public interface LatencyFaultTolerance<T> {

    /**
     * 更新错误条目
     *
     * @param name brokerName（目前只用于这一种场景）
     * @param currentLatency 当前消息发送消耗的时间
     * @param notAvailableDuration 不可用持续时间，这段时间内broker被规避
     */
    void updateFaultItem(final T name, final long currentLatency, final long notAvailableDuration);

    /**
     * 判断当前broker是否可用
     *
     * @param name brokerName
     * @return
     */
    boolean isAvailable(final T name);

    /**
     * 释放broker，broker可以重新参与路由选择
     *
     * @param name
     */
    void remove(final T name);

    /**
     * 从已经被规避（屏蔽）的broker中选取一个相对可用性能较高的，如果一个都没有返回null
     *
     * @return
     */
    T pickOneAtLeast();
}
