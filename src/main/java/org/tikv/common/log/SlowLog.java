/*
 * Copyright 2021 TiKV Project Authors.
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
 *
 */

package org.tikv.common.log;

public interface SlowLog {
  SlowLogSpan start(String name);

  long getTraceId();

  long getThresholdMS();

  void setError(Throwable err);

<<<<<<< HEAD
=======
  SlowLog withFields(Map<String, Object> fields);

  default SlowLog withField(String key, Object value) {
    return withFields(ImmutableMap.of(key, value));
  }

  Object getField(String key);

>>>>>>> 6cbf56aed... [to #556] metrics: attach cluster label to metrics (#558)
  void log();
}
