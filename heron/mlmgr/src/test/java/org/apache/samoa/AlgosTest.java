package org.apache.samoa;

/*
 * #%L
 * SAMOA
 * %%
 * Copyright (C) 2014 - 2015 Apache Software Foundation
 * %%
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *      http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * #L%
 */

import org.apache.samoa.LocalStormDoTask;
import org.apache.samoa.TestParams;
import org.apache.samoa.TestUtils;
import org.junit.Test;

public class AlgosTest {

  @Test(timeout = 60000)
  public void testVHTWithStorm() throws Exception {

    TestParams vhtConfig = new TestParams.Builder()
        .inputInstances(200_000)
        .samplingSize(20_000)
        .evaluationInstances(200_000)
        .classifiedInstances(200_000)
        .labelSamplingSize(10l)
        .classificationsCorrect(55f)
        .kappaStat(-0.1f)
        .kappaTempStat(-0.1f)
        .cliStringTemplate(TestParams.Templates.PREQEVAL_VHT_RANDOMTREE)
        .resultFilePollTimeout(30)
        .prePollWait(15)
        .taskClassName(LocalStormDoTask.class.getName())
        .build();
    TestUtils.test(vhtConfig);

  }

  @Test(timeout = 120000)
  public void testBaggingWithStorm() throws Exception {
    TestParams baggingConfig = new TestParams.Builder()
        .inputInstances(200_000)
        .samplingSize(20_000)
        .evaluationInstances(180_000)
        .classifiedInstances(190_000)
        .labelSamplingSize(10l)
        .classificationsCorrect(60f)
        .kappaStat(0f)
        .kappaTempStat(0f)
        .cliStringTemplate(TestParams.Templates.PREQEVAL_BAGGING_RANDOMTREE)
        .resultFilePollTimeout(40)
        .prePollWait(20)
        .taskClassName(LocalStormDoTask.class.getName())
        .build();
    TestUtils.test(baggingConfig);

  }

  @Test(timeout = 240000)
  public void testCVPReqVHTWithStorm() throws Exception {

    TestParams vhtConfig = new TestParams.Builder()
        .inputInstances(200_000)
        .samplingSize(20_000)
        .evaluationInstances(200_000)
        .classifiedInstances(200_000)
        .classificationsCorrect(55f)
        .kappaStat(0f)
        .kappaTempStat(0f)
        .cliStringTemplate(TestParams.Templates.PREQCVEVAL_VHT_RANDOMTREE)
        .resultFilePollTimeout(30)
        .prePollWait(15)
        .taskClassName(LocalStormDoTask.class.getName())
        .labelFileCreated(false)
        .build();
    TestUtils.test(vhtConfig);

  }

}
