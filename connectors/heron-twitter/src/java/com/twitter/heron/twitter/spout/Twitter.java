package com.streamlio.connectors.twitter;

import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.google.common.base.Preconditions;

import twitter4j.FilterQuery;
import twitter4j.StallWarning;
import twitter4j.Status;
import twitter4j.StatusDeletionNotice;
import twitter4j.StatusListener;
import twitter4j.TwitterStream;
import twitter4j.TwitterStreamFactory;
import twitter4j.auth.AccessToken;
import twitter4j.conf.ConfigurationBuilder;

import com.twitter.heron.api.Config;
import com.twitter.heron.api.topology.TopologyContext;
import com.twitter.heron.api.topology.OutputFieldsDeclarer;
import com.twitter.heron.api.spout.BaseRichSpout;
import com.twitter.heron.api.spout.SpoutOutputCollector;
import com.twitter.heron.api.tuple.Fields;
import com.twitter.heron.api.tuple.Values;
import com.twitter.heron.api.utils.Utils;

@SuppressWarnings("serial")
public class Twitter extends BaseRichSpout {
   private static final long serialVersionUID = 4322775001819135036L;
   private static final Logger LOG = Logger.getLogger(Twitter.class.getName());

   private String componentId;
   private String spoutId;
   private SpoutOutputCollector collector;

   private Authentication authInfo;

   private LinkedBlockingQueue<Status> queue = null;
   private TwitterStream twitterStream;

   public Twitter(Authentication authInfo) {
     this.authInfo = authInfo;
   }

   @Override
   public void open(Map conf, TopologyContext context, SpoutOutputCollector spoutOutputCollector) {
     this.componentId = context.getThisComponentId();
     this.spoutId = String.format("%s-%s", componentId, context.getThisTaskId());
     this.collector = spoutOutputCollector;

     this.queue = new LinkedBlockingQueue<Status>(1000);
     StatusListener listener = new StatusListener() {
       @Override
       public void onStatus(Status status) {
         queue.offer(status);
       }
					
       @Override
       public void onDeletionNotice(StatusDeletionNotice sdn) {}
					
       @Override
       public void onTrackLimitationNotice(int i) {}
					
       @Override
       public void onScrubGeo(long l, long l1) {}
					
       @Override
       public void onException(Exception ex) {}
					
       @Override
       public void onStallWarning(StallWarning arg0) {
         // TODO Auto-generated method stub
       }
     };
				
     ConfigurationBuilder cb = new ConfigurationBuilder();
     cb.setDebugEnabled(true)
       .setOAuthConsumerKey(authInfo.getConsumerKey())
       .setOAuthConsumerSecret(authInfo.getConsumerSecret())
       .setOAuthAccessToken(authInfo.getAccessToken())
       .setOAuthAccessTokenSecret(authInfo.getAccessTokenSecret());
					
     twitterStream = new TwitterStreamFactory(cb.build()).getInstance();
     twitterStream.addListener(listener);
				
     if (authInfo.getKeyWords().length == 0) {
       twitterStream.sample();
     } else {
       FilterQuery query = new FilterQuery().track(authInfo.getKeyWords());
       twitterStream.filter(query);
     }
   }
			
   @Override
   public void nextTuple() {
     Status ret = queue.poll();
     if (ret == null) {
       Utils.sleep(50);
     } else {
       collector.emit(new Values(ret));
     }
   }
			
   @Override
   public void close() {
     super.close();
     twitterStream.shutdown();
   }
			
   @Override
   public void ack(Object id) {}
			
   @Override
   public void fail(Object id) {}
			
   @Override
   public void declareOutputFields(OutputFieldsDeclarer declarer) {
     declarer.declare(new Fields("tweet"));
   }
}
