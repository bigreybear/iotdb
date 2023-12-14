/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iotdb.pathgenerator;

public enum MeasurementName {
  mainsStatusAlarm,
  lv1PowerOffStatus,
  lv2PowerOffStatus,
  subPhalanxVoltage1,
  subPhalanxCurrent1,
  subPhalanxVoltage2,
  subPhalanxCurrent2,
  subPhalanxVoltage3,
  subPhalanxCurrent3,
  subPhalanxVoltage4,
  subPhalanxCurrent4,
  subPhalanxVoltage5,
  subPhalanxCurrent5,
  subPhalanxVoltage6,
  subPhalanxCurrent6,
  controllerOutVoltage,
  controllerTemperature,
  batteryInOutCurrent,
  batteryVoltage,
  batteryCapacity,
  batteryTemperature,
  current,
  rectifierInVoltage,
  fanVoltage1,
  fanCurrent1,
  rectifierOutCurrent1,
  photovoltaicDailyPower1,
  photovoltaicSumPower1,
  windDailyPower1,
  windSumPower1,
  fanVoltage2,
  fanCurrent2,
  rectifierOutCurrent2,
  photovoltaicDailyPower2,
  photovoltaicSumPower2,
  windDailyPower2,
  windSumPower2,
  fanVoltage3,
  fanCurrent3,
  rectifierOutCurrent3,
  photovoltaicDailyPower3,
  photovoltaicSumPower3,
  windDailyPower3,
  windSumPower3,
  fanVoltage4,
  fanCurrent4,
  rectifierOutCurrent4,
  photovoltaicDailyPower4,
  photovoltaicSumPower4,
  windDailyPower4,
  windSumPower4,
  fanVoltage5,
  fanCurrent5,
  rectifierOutCurrent5,
  photovoltaicDailyPower5,
  photovoltaicSumPower5,
  windDailyPower5,
  windSumPower5;

  @Override
  public String toString() {
    return name();
  }
}
