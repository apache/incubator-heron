package com.twitter.heron.common.basics;

import java.nio.channels.SelectableChannel;

/**
 * Implementing this interface allows an object to be the callback of SelectServer
 */

public interface ISelectHandler {
  /**
   * Handle a SelectableChannel when it is readable
   *
   * @param channel the channel ISelectHandler with handle with
   */
  void handleRead(SelectableChannel channel);

  /**
   * Handle a SelectableChannel when it is writable
   *
   * @param channel the channel ISelectHandler with handle with
   */
  void handleWrite(SelectableChannel channel);

  /**
   * Handle a SelectableChannel when it is acceptable
   *
   * @param channel the channel ISelectHandler with handle with
   */
  void handleAccept(SelectableChannel channel);

  /**
   * Handle a SelectableChannel when it is connectible
   *
   * @param channel the channel ISelectHandler with handle with
   */
  void handleConnect(SelectableChannel channel);

  /**
   * Handle a SelectableChannel when it meets some errors
   *
   * @param channel the channel ISelectHandler with handle with
   */
  void handleError(SelectableChannel channel);
}
