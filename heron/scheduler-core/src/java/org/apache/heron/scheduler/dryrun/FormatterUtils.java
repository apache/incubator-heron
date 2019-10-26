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

package org.apache.heron.scheduler.dryrun;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import com.google.common.base.Optional;
import com.google.common.base.Strings;

import org.apache.heron.common.basics.ByteAmount;
import org.apache.heron.spi.packing.PackingPlan;
import org.apache.heron.spi.packing.Resource;

/**
 * Formatter utilities
 */
public final class FormatterUtils {

  public FormatterUtils(boolean rich) {
    this.rich = rich;
  }

  /**
   * If render in rich format (with color and text style)
   */
  private final boolean rich;

  /**
   * Simple and self-contained support of using ANSI escape codes.
   *
   * @see <a href="https://en.wikipedia.org/wiki/ANSI_escape_code">ANSI escape code</a>
   *
   */
  private static final String ANSI_RESET = "\u001B[0m";
  private static final String ANSI_RED = "\u001B[31m";
  private static final String ANSI_GREEN = "\u001B[32m";
  private static final String ANSI_BOLD = "\u001B[1m";

  /*
   * Unicode of long strike overlay. A character followed by \u0036 will
   * be rendered on terminal as itself being struck through
   *
   * See: http://unicode.org/charts/PDF/U0300.pdf
   */
  private static final String STRIKETHROUGH = "\u0336";

  public enum TextColor {
    DEFAULT,
    RED,
    GREEN
  }

  public enum TextStyle {
    DEFAULT,
    BOLD,
    STRIKETHROUGH
  }

  public enum ContainerChange {
    UNAFFECTED,
    MODIFIED,
    NEW,
    REMOVED
  }


  /**
   * Poor man's tabulate implementation
   *
   * Each tabulates consists of a list of rows. Each row consists of a list of cells.
   *
   */

  /**
   * Cell is the smallest unit in a tabulate. More generally, it is a class
   * that represents a piece of text with style and color added.
   */
  public static class Cell {
    // Text in the cell
    private final String text;

    // Length of the text. It is used to calculate the proper widht of a column
    private final int length;
    private String formatter;
    private TextColor color;
    private TextStyle style;


    public Cell(String text) {
      this.text = text;
      this.length = text.length();
      this.formatter = "%s";
      this.color = TextColor.DEFAULT;
      this.style = TextStyle.DEFAULT;
    }

    public Cell(String text, TextColor color) {
      this(text);
      this.color = color;
    }

    public Cell(String text, TextStyle style) {
      this(text);
      this.style = style;
    }

    public Cell(String text, TextColor color, TextStyle style) {
      this(text);
      this.color = color;
      this.style = style;
    }

    public void setColor(TextColor color) {
      this.color = color;
    }

    public void setStyle(TextStyle style) {
      this.style = style;
    }

    public void setFormatter(String formatter) {
      this.formatter = formatter;
    }

    public final int getLength() {
      return this.length;
    }

    /**
     * Convert Cell to String
     * @param rich if render in rich format
     * @return the cell string
     */
    public String toString(boolean rich) {
      StringBuilder builder = new StringBuilder();
      String formattedText = String.format(formatter, text);
      if (rich) {
        switch (style) {
          case BOLD:
            builder.append(ANSI_BOLD);
            builder.append(formattedText);
            break;
        /* Adding strike-through effect to a string is different. One needs to append unicode of
           long strikethrough overlay to each single character in a string of characters. */
          case STRIKETHROUGH:
            for (int i = 0; i < formattedText.length(); i++) {
              builder.append(formattedText.charAt(i));
              builder.append(STRIKETHROUGH);
            }
            break;
          case DEFAULT:
            builder.append(formattedText);
            break;
          default:
            throw new RuntimeException("Unknown text style: " + style);
        }
        switch (color) {
          case RED:
            builder.insert(0, ANSI_RED);
            break;
          case GREEN:
            builder.insert(0, ANSI_GREEN);
            break;
          case DEFAULT:
            break;
          default:
            throw new RuntimeException("Unknown text color: " + color);
        }
        // Only append ANSI reset escape code if text style or text color is added
        if (style != TextStyle.DEFAULT || color != TextColor.DEFAULT) {
          builder.append(ANSI_RESET);
        }
      } else {
        builder.append(formattedText);
      }
      return builder.toString();
    }
  }

  /**
   * Row, which consists a list of cells.
   *
   * <pre>
   *   ----------------------
   *   | xxxx | yyyy | zzzz | &lt;- a list of cells
   *   ----------------------
   *                 ^
   *                 |------- separator: "|"
   * </pre>
   */
  public static class Row {
    private List<Cell> row;
    private static final String SEPARATOR = "|";

    public Row(List<String> row) {
      this.row = new ArrayList<>();
      for (String text: row) {
        this.row.add(new Cell(text));
      }
    }

    /**
     * Set color for a list of cells in a row
     * @param color
     */
    public void setColor(TextColor color) {
      for (Cell cell: row) {
        cell.setColor(color);
      }
    }

    /**
     * Set style for a list of cells in a row
     * @param style
     */
    public void setStyle(TextStyle style) {
      for (Cell cell: row) {
        cell.setStyle(style);
      }
    }

    /**
     * Set formatter for each cell in the row
     * @param formatters
     */
    public void setFormatters(List<String> formatters) {
      for (int i = 0; i < formatters.size(); i++) {
        row.get(i).setFormatter(formatters.get(i));
      }
    }

    public int size() {
      return row.size();
    }

    /**
     * Get length of the cell at index {@code index} in the row
     * @param index
     * @return length of the cell
     */
    public Cell getCell(int index) {
      return row.get(index);
    }

    public String toString(boolean rich) {
      List<String> renderedCells = new ArrayList<>();
      for (Cell c: row) {
        renderedCells.add(c.toString(rich));
      }
      return String.format("%s %s %s",
          SEPARATOR, String.join(" " +  SEPARATOR + " ", renderedCells), SEPARATOR);
    }
  }


  /**
   * Table, which consists of a title and a list of rows below the title
   *
   * <pre>
   *     ============================
   *     | title1 | title2 | title3 | &lt;----- title
   *     ============================
   *     | xxxxxx | yyy    | zzzzzz | &lt;-|
   *     ----------------------------   |----- rows
   *     | gggg   | uuuuu  | ooo    | &lt;-|
   * </pre>
   */
  public static class Table {
    private Row title;
    private List<Row> rows;

    public Table(Row title, List<Row> rows) {
      this.title = title;
      this.rows = rows;
    }

    private StringBuilder addRow(StringBuilder builder, String row) {
      builder.append(row);
      builder.append('\n');
      return builder;
    }

    /**
     * Calculate proper width for each column.
     *
     * Notice that if a cell contains text "foo" in red and bold style, the internal
     * representation will be "\u001B[31m\u001B[1mfoo\u001B[0m". However during the
     * calculation, ANSI escape codes/Unicode overlay should not be counted because they only
     * serve as visual effect and do not take extra space on terminal.
     *
     * @return a list of integers specifying proper length of each column
     */
    private List<Integer> calculateColumnsMax() {
      List<Integer> width = new ArrayList<>();
      for (int i = 0; i < title.size(); i++) {
        width.add(title.getCell(i).getLength());
      }
      for (Row row: rows) {
        for (int i = 0; i < row.size(); i++) {
          width.set(i, Math.max(width.get(i), row.getCell(i).getLength()));
        }
      }
      return width;
    }
    /**
     * Generate formatter for each row based on rows. Width of a column is the
     * max width of all cells on that column.
     *
     * Explanation of the {@code metaCellFormatter}:
     *
     *  If the column max width is 8, then {@code String.format(metaCellFormatter, 8)}
     *  gives us {@code "%8s"}
     *
     *  @see <a href="https://docs.oracle.com/javase/8/docs/api/java/util/Formatter.html">
     *    Formatter</a>
     *
     * @return formatter for rows
     */
    private List<String> generateRowFormatter() {
      List<String> formatters = new ArrayList<>();
      List<Integer> columnsMax = calculateColumnsMax();
      String metaCellFormatter = "%%%ds";
      for (Integer width: columnsMax) {
        formatters.add(String.format(metaCellFormatter, width));
      }
      return formatters;
    }

    /**
     * Generate length of table frame
     *
     * Definition of table frame:
     *
     *     =============================== &lt;- table frame
     *     | xxxxx | yyyyy | zzzzz | sss |
     *     ===============================
     *     | ..... | ..... | ..... | ... |
     *
     * The constant 3 comes from two spaces surrounding the text
     * and one separator after the text.
     *
     *     space
     *      |
     *     ---------
     *     | xxxxx | &lt;- one separator
     *     ---------
     *            |
     *           space
     *
     * The final constant 1 is the leftmost separator of a row
     *
     * @return
     */
    private int calculateFrameLength() {
      int total = 0;
      for (Integer width: calculateColumnsMax()) {
        total += width + 3;
      }
      return total + 1;
    }

    /**
     * Format rows and title into a table
     * @param rich if render table in rich format
     * @return Formatted table
     */
    public String createTable(boolean rich) {
      // Generate formatter for each cell in a single row
      List<String> formatters = generateRowFormatter();
      // Set formatter for each row
      title.setFormatters(formatters);
      for (Row row: rows) {
        row.setFormatters(formatters);
      }
      // Calculate length for frames
      int frameLength = calculateFrameLength();
      // Start building table
      StringBuilder builder = new StringBuilder();
      // Add upper frame
      addRow(builder, Strings.repeat("=", frameLength));
      // Add title
      addRow(builder, title.toString(rich));
      // Add one single line to separate title and content
      addRow(builder, Strings.repeat("-", frameLength));
      // Add each row
      for (Row row: rows) {
        addRow(builder, row.toString(rich));
      }
      // Add lower frame
      addRow(builder, Strings.repeat("=", frameLength));
      return builder.toString();
    }
  }

  private static final List<String> TITLE_NAMES = Arrays.asList(
        "component", "task ID", "CPU", "RAM (MB)", "disk (MB)");


  /******************************** Auxiliary functions ********************************/

  /**
   * Format new amount associated with change in percentage
   * For example, with {@code oldAmount = 2} and {@code newAmount = 1}
   * the result Cell is " -50.00%" (in red color)
   * @param oldAmount old resource usage
   * @param newAmount new resource usage
   * @return formatted chagne in percentage if oldAmount and newAmount differ
   */
  public static Optional<Cell> percentageChange(double oldAmount, double newAmount) {
    double delta = newAmount - oldAmount;
    double percentage = delta / oldAmount * 100.0;
    if (percentage == 0.0) {
      return Optional.absent();
    } else {
      String sign = "";
      if (percentage > 0.0) {
        sign = "+";
      }
      Cell cell = new Cell(String.format("%s%.2f%%", sign, percentage));
      // set color to red if percentage drops, to green if percentage increases
      if ("".equals(sign)) {
        cell.setColor(TextColor.RED);
      } else {
        cell.setColor(TextColor.GREEN);
      }
      return Optional.of(cell);
    }
  }

  public String renderContainerName(Integer containerId) {
    return new Cell(String.format("Container %d", containerId),
              FormatterUtils.TextStyle.BOLD).toString(rich);
  }

  public String renderContainerChange(ContainerChange change) {
    Cell c = new Cell(change.toString());
    switch (change) {
      case NEW:
        c.setColor(TextColor.GREEN);
        break;
      case REMOVED:
        c.setColor(TextColor.RED);
        break;
      default:
        break;
    }
    return c.toString(rich);
  }

  public Row rowOfInstancePlan(PackingPlan.InstancePlan plan,
                                      TextColor color, TextStyle style) {
    String taskId = String.valueOf(plan.getTaskId());
    String cpu = String.valueOf(plan.getResource().getCpu());
    String ram = String.valueOf(plan.getResource().getRam().asMegabytes());
    String disk = String.valueOf(plan.getResource().getDisk().asMegabytes());
    List<String> cells = Arrays.asList(
          plan.getComponentName(), taskId, cpu, ram, disk);
    Row row = new Row(cells);
    row.setStyle(style);
    row.setColor(color);
    return row;
  }

  public String renderOneContainer(List<Row> rows) {
    Row title = new Row(TITLE_NAMES);
    title.setStyle(TextStyle.BOLD);
    return new Table(title, rows).createTable(rich);
  }

  public String renderResourceUsage(Resource resource) {
    double cpu = resource.getCpu();
    ByteAmount ram = resource.getRam();
    ByteAmount disk = resource.getDisk();
    return String.format("CPU: %s, RAM: %s MB, Disk: %s MB",
      cpu, ram.asMegabytes(), disk.asMegabytes());
  }

  public String renderResourceUsageChange(Resource oldResource, Resource newResource) {
    double oldCpu = oldResource.getCpu();
    double newCpu = newResource.getCpu();
    Optional<Cell> cpuUsageChange = FormatterUtils.percentageChange(oldCpu, newCpu);
    long oldRam = oldResource.getRam().asMegabytes();
    long newRam = newResource.getRam().asMegabytes();
    Optional<Cell> ramUsageChange = FormatterUtils.percentageChange(oldRam, newRam);
    long oldDisk = oldResource.getDisk().asMegabytes();
    long newDisk = newResource.getDisk().asMegabytes();
    Optional<Cell> diskUsageChange = FormatterUtils.percentageChange(oldDisk, newDisk);
    String cpuUsage = String.format("CPU: %s", newCpu);
    if (cpuUsageChange.isPresent()) {
      cpuUsage += String.format(" (%s)", cpuUsageChange.get().toString(rich));
    }
    String ramUsage = String.format("RAM: %s MB", newRam);
    if (ramUsageChange.isPresent()) {
      ramUsage += String.format(" (%s)", ramUsageChange.get().toString(rich));
    }
    String diskUsage = String.format("Disk: %s MB", newDisk);
    if (diskUsageChange.isPresent()) {
      diskUsage += String.format(" (%s)", diskUsageChange.get().toString(rich));
    }
    return String.join(", ", cpuUsage, ramUsage, diskUsage);
  }
}
