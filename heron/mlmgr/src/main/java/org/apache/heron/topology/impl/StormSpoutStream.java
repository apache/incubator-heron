package org.apache.samoa.topology.impl;
//package org.apache.samoa.topology.impl;

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
//
//import org.apache.samoa.core.ContentEvent;
//import org.apache.samoa.topology.impl.StormEntranceProcessingItem.StormEntranceSpout;
//
///**
// * Storm Stream that connects into Spout. It wraps the spout itself
// * @author Arinto Murdopo
// *
// */
//final class StormSpoutStream extends StormStream{
//
//	/**
//	 * 
//	 */
//	private static final long serialVersionUID = -7444653177614988650L;
//	
//	private StormEntranceSpout spout;
//	
//	StormSpoutStream(String stormComponentId) {
//		super(stormComponentId);
//	}
//
//	@Override
//	public void put(ContentEvent contentEvent) {
//		spout.put(this, contentEvent);
//	}
//	
//    void setSpout(StormEntranceSpout spout){
//		this.spout = spout;
//	}
//
////	@Override
////	public void setStreamId(String stream) {
////		// TODO Auto-generated method stub
////		
////	}
//
//	@Override
//	public String getStreamId() {
//		// TODO Auto-generated method stub
//		return null;
//	}
//
// }
