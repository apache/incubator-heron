package com.twitter.heron.common.utils.misc;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InvalidObjectException;
import java.io.ObjectStreamException;
import java.io.PrintStream;
import java.util.logging.FileHandler;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;

import com.twitter.heron.common.config.SystemConfig;
import com.twitter.heron.common.basics.Constants;
import com.twitter.heron.common.basics.SingletonRegistry;

/**
 * A helper class to init corresponding LOGGER setting
 */
public class LoggingHelper {
  static public void loggerInit(String instanceId, Level level, String loggingDir) throws IOException {
    // For now we would just consider the FileHandler case

    // The Logger.getLogger("") will get the root Logger, which means, when we create other
    // Loggers from LoggerFactory, they will follow root Logger's configuration.
    Logger.getLogger("").setLevel(level);
    Logger.getLogger("").addHandler(LoggingHelper.getFileHandler(instanceId, loggingDir));
    Logger.getLogger("").addHandler(new ErrorReportLoggingHandler());

    // now rebind stdout/stderr to logger
    Logger logger;
    LoggingOutputStream los;

    logger = Logger.getLogger("stdout");
    los = new LoggingOutputStream(logger, StdOutErrLevel.STDOUT);
    System.setOut(new PrintStream(los, true));

    logger = Logger.getLogger("stderr");
    los = new LoggingOutputStream(logger, StdOutErrLevel.STDERR);
    System.setErr(new PrintStream(los, true));
  }

  static public FileHandler getFileHandler(String instanceId, String loggingDir) throws IOException {
    SystemConfig systemConfig = (SystemConfig) SingletonRegistry.INSTANCE.getSingleton(
        SystemConfig.HERON_SYSTEM_CONFIG);
    // The pattern of file name should be:
    // instance-${instanceId}.log.index
    String pattern = loggingDir + "/" + instanceId + ".log.%g";
    boolean append = true;
    int limit = systemConfig.getHeronLoggingMaximumSizeMb() * Constants.MB_TO_BYTES;
    int count = systemConfig.getHeronLoggingMaximumFiles();

    FileHandler fileHandler = new FileHandler(pattern, limit, count, append);
    fileHandler.setFormatter(new SimpleFormatter());
    fileHandler.setEncoding("UTF-8");

    return fileHandler;
  }

  public static class StdOutErrLevel extends Level {
    /**
     * Private constructor
     */
    private StdOutErrLevel(String name, int value) {
      super(name, value);
    }

    /**
     * Level for STDOUT activity.
     */
    public static Level STDOUT =
        new StdOutErrLevel("STDOUT", Level.INFO.intValue() + 53);
    /**
     * Level for STDERR activity
     */
    public static Level STDERR =
        new StdOutErrLevel("STDERR", Level.INFO.intValue() + 54);

    /**
     * Method to avoid creating duplicate instances when deserializing the
     * object.
     *
     * @return the singleton instance of this <code>Level</code> value in this
     * classloader
     * @throws java.io.ObjectStreamException If unable to deserialize
     */
    protected Object readResolve()
        throws ObjectStreamException {
      if (this.intValue() == STDOUT.intValue())
        return STDOUT;
      if (this.intValue() == STDERR.intValue())
        return STDERR;
      throw new InvalidObjectException("Unknown instance :" + this);
    }
  }

  /**
   * An OutputStream that writes contents to a Logger upon each call to flush()
   */
  public static class LoggingOutputStream extends ByteArrayOutputStream {

    private String lineSeparator;

    private Logger logger;
    private Level level;

    /**
     * Constructor
     *
     * @param logger Logger to write to
     * @param level Level at which to write the log message
     */
    public LoggingOutputStream(Logger logger, Level level) {
      super();
      this.logger = logger;
      this.level = level;
      lineSeparator = System.getProperty("line.separator");
    }

    /**
     * upon flush() write the existing contents of the OutputStream
     * to the logger as a log record.
     *
     * @throws java.io.IOException in case of error
     */
    public void flush() throws IOException {

      String record;
      synchronized (this) {
        super.flush();
        record = this.toString();
        super.reset();

        if (record.length() == 0 || record.equals(lineSeparator)) {
          // avoid empty records
          return;
        }

        logger.logp(level, "", "", record);
      }
    }
  }
}
