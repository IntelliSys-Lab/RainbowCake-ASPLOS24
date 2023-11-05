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

package org.apache.openwhisk.core.entity

object Annotations {
  val FinalParamsAnnotationName = "final"
  val WebActionAnnotationName = "web-export"
  val WebCustomOptionsAnnotationName = "web-custom-options"
  val RawHttpAnnotationName = "raw-http"
  val RequireWhiskAuthAnnotation = "require-whisk-auth"
  val ProvideApiKeyAnnotationName = "provide-api-key"
  val InvokerResourcesAnnotationName = "invoker-resources"
  val InvokerResourcesStrictPolicyAnnotationName = "invoker-resources-strict-policy"
}
