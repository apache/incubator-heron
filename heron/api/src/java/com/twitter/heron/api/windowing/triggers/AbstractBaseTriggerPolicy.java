package com.twitter.heron.api.windowing.triggers;

import java.io.Serializable;
import java.util.Map;

import com.twitter.heron.api.windowing.EvictionPolicy;
import com.twitter.heron.api.windowing.TriggerHandler;
import com.twitter.heron.api.windowing.TriggerPolicy;
import com.twitter.heron.api.windowing.WindowManager;

public abstract class AbstractBaseTriggerPolicy<T extends Serializable, S>
        implements TriggerPolicy<T , S> {
    protected TriggerHandler handler;
    protected EvictionPolicy<T, ?> evictionPolicy;
    protected WindowManager<T> windowManager;
    protected Boolean started;
    protected Map<String, Object> topoConf;

    /**
     * Set the eviction policy to whatever eviction policy to use this with
     *
     * @param evictionPolicy the eviction policy
     */
    public void setEvictionPolicy(EvictionPolicy<T, ?> evictionPolicy){
        this.evictionPolicy = evictionPolicy;
    }

    /**
     * Set the trigger handler for this trigger policy to trigger
     *
     * @param triggerHandler the trigger handler
     */
    public void setTriggerHandler(TriggerHandler triggerHandler){
        this.handler = triggerHandler;
    }

    /**
     * Sets the window manager that uses this trigger policy
     *
     * @param windowManager the window manager
     */
    public void setWindowManager(WindowManager<T> windowManager){
        this.windowManager = windowManager;
    }

    /**
     * Sets the Config used for this topology
     *
     * @param config the configuration object
     */
    public void setTopologyConfig(Map<String, Object> config){
        this.topoConf = config;
    }

    /**
     * Starts the trigger policy. This can be used
     * during recovery to start the triggers after
     * recovery is complete.
     */
    public void start(){
        if(this.evictionPolicy == null){
            throw new RuntimeException("EvictionPolicy of TriggerPolicy was not set.");
        }

        if(this.handler == null){
            throw new RuntimeException("TriggerHandler of TriggerPolicy was not set.");
        }

        if(this.windowManager == null){
            throw new RuntimeException("WindowManager of TriggerPolicy was not set.");
        }

        started = true;
    }
}
