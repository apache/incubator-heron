//  Copyright 2017 Twitter. All rights reserved.
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//  http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License
package com.twitter.heron.scheduler.dryrun;

import java.util.ArrayList;
import java.util.List;

import com.google.common.base.Optional;
import com.google.common.base.Strings;

/**
 * Formatter utilities
 */
public class FormatterUtils {

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
    GREEN;
  }

  public enum TextStyle {
    DEFAULT,
    BOLD,
    STRIKETHROUGH;
  }

  public static class Cell {
    private final String text;
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

    public String toString() {
      StringBuilder builder = new StringBuilder();
      String formattedText = String.format(formatter, text);
      switch (style) {
        case BOLD:
          builder.append(ANSI_BOLD);
          builder.append(formattedText);
          break;
        case STRIKETHROUGH:
          for (int i = 0; i < formattedText.length(); i++) {
            builder.append(formattedText.charAt(i));
            builder.append(STRIKETHROUGH);
          }
          break;
        case DEFAULT:
          builder.append(formattedText);
          break;
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
      }
      if (style != TextStyle.DEFAULT || color != TextColor.DEFAULT) {
        builder.append(ANSI_RESET);
      }
      return builder.toString();
    }
  }

  public static class Row {
    private List<Cell> row;
    private static final String separator = "|";

    public Row(List<String> row) {
      this.row = new ArrayList<>();
      for(String text: row) {
        this.row.add(new Cell(text));
      }
    }

    public void setColor(TextColor color) {
      for(Cell cell: row) {
        cell.setColor(color);
      }
    }

    public void setStyle(TextStyle style) {
      for(Cell cell: row) {
        cell.setStyle(style);
      }
    }

    public void setFormatters(List<String> formatters) {
      for(int i = 0; i < formatters.size(); i++) {
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

    public String toString() {
      List<String> renderedCells = new ArrayList<>();
      for(Cell c: row) {
        renderedCells.add(c.toString());
      }
      return String.format("%s %s %s",
          separator, String.join(" " +  separator + " ", renderedCells), separator);
    }
  }

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
     * generate formatter for each row based on rows. Width of a column is the
     * max width of all cells on that column
     *
     * Explanation of the {@code metaCellFormatter}:
     *
     *  If the column max width is 8, then {@code String.format(metaCellFormatter, 8)}
     *  gives us {@code "%8s"}
     *
     *  @see <a href="https://docs.oracle.com/javase/8/docs/api/java/util/Formatter.html">
     *    Formatter</a>
     *
     * @return formatter for row
     */
    private List<String> generateRowFormatter() {
      List<String> formatters = new ArrayList<>();
      List<Integer> columnsMax = calculateColumnsMax();
      String metaCellFormatter = "%%%ds";
      for(Integer width: columnsMax) {
        formatters.add(String.format(metaCellFormatter, width));
      }
      return formatters;
    }

    /**
     * Generate length of table frame
     *
     * The constant 3 comes from two spaces surrounding the text
     * and one separator after the text.
     *
     *     space
     *      |
     *     ---------
     *     | xxxxx | <- one separator
     *     ---------
     *            |
     *           space
     *
     * The final constant 1 is the leftmost separator of a row
     *
     * @return
     */
    private int caculateFrameLength() {
      int total = 0;
      for(Integer width: calculateColumnsMax()) {
        total += width + 3;
      }
      return total + 1;
    }

    /**
     * Format rows and title into a table
     * @return Formatted table
     */
    public String createTable() {
      // Generate formatter for each cell in a single row
      List<String> formatters = generateRowFormatter();
      // Set formatter for each row
      title.setFormatters(formatters);
      for(Row row: rows) {
        row.setFormatters(formatters);
      }
      // Calculate length for frames
      int frameLength = caculateFrameLength();
      // Start building table
      StringBuilder builder = new StringBuilder();
      // Add upper frame
      addRow(builder, Strings.repeat("=", frameLength));
      // Add title
      addRow(builder, title.toString());
      // Add one single line to separate title and content
      addRow(builder, Strings.repeat("-", frameLength));
      // Add each row
      for(Row row: rows) {
        addRow(builder, row.toString());
      }
      // Add lower frame
      addRow(builder, Strings.repeat("=", frameLength));
      return builder.toString();
    }
  }

  /*
   * Format new amount associated with change in percentage
   * For example, with {@code oldAmount = 2} and {@code newAmount = 1}
   * the result string is " 1 (-50.00%)"
   * @param oldAmount old amount
   * @param newAmount new amount
   * @return formatted change
   */

  /**
   * Format new amount associated with change in percentage
   * For example, with {@code oldAmount = 2} and {@code newAmount = 1}
   * the result Cell is " -50.00%" (in red color)
   * @param oldAmount old resource usage
   * @param newAmount new resource usage
   * @return formatted chagne in percentage if oldAmount and newAmount differ
   */
  protected static Optional<Cell> percentageChange(double oldAmount, double newAmount) {
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
      if (sign == "") {
        cell.setColor(TextColor.RED);
      } else {
        cell.setColor(TextColor.GREEN);
      }
      return Optional.of(cell);
    }
  }
}
