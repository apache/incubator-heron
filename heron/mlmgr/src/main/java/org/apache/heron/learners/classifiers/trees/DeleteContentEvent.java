package org.apache.samoa.learners.classifiers.trees;

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

/**
 * Delete Content Event is the content event that is sent by Model Aggregator Processor to delete unnecessary statistic
 * in Local Statistic Processor.
 * 
 * @author Arinto Murdopo
 * 
 */
final class DeleteContentEvent extends ControlContentEvent {

  private static final long serialVersionUID = -2105250722560863633L;

  public DeleteContentEvent() {
    super(-1);
  }

  DeleteContentEvent(long id) {
    super(id);
  }

  @Override
  LocStatControl getType() {
    return LocStatControl.DELETE;
  }

}
