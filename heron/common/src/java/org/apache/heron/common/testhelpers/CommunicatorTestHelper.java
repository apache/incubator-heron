/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.heron.common.testhelpers;

import java.util.concurrent.CountDownLatch;

import org.mockito.Matchers;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import org.apache.heron.common.basics.Communicator;

import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.spy;

/**
 * Communicator test helper can spy on a communicator to  provide the ability to await a certain
 * number of expected offers to be received before proceeding.
 */
public final class CommunicatorTestHelper {

  private CommunicatorTestHelper() {
  }

  public static <T> Communicator<T> spyCommunicator(Communicator<T> communicator,
                                                    final CountDownLatch offerLatch) {
    Communicator<T> returnVal = spy(communicator);
    doAnswer(new Answer<Boolean>() {
      @Override
      public Boolean answer(InvocationOnMock invocationOnMock) throws Throwable {
        Boolean returnBoolean = (Boolean) invocationOnMock.callRealMethod();
        offerLatch.countDown();
        return returnBoolean;
      }
    }).when(returnVal).offer(Matchers.<T>any());
    return returnVal;
  }
}
