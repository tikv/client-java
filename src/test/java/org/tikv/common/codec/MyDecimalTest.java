/*
 *
 * Copyright 2017 TiKV Project Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package org.tikv.common.codec;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.List;
import org.junit.Test;

public class MyDecimalTest {
  @Test
  public void fromStringTest() {
    List<MyDecimalTestStruct> tests = new ArrayList<>();
    tests.add(
        new MyDecimalTestStruct(
            "1111111111111111111111111.111111111111111111111111111111",
            "1111111111111111111111111.111111111111111111111111111111",
            65,
            30));
    tests.add(new MyDecimalTestStruct("12345", "12345", 5, 0));
    tests.add(new MyDecimalTestStruct("12345.", "12345", 5, 0));
    tests.add(new MyDecimalTestStruct("123.45", "123.45", 5, 2));
    tests.add(new MyDecimalTestStruct("-123.45", "-123.45", 5, 2));
    tests.add(new MyDecimalTestStruct(".00012345000098765", "0.00012345000098765", 17, 17));
    tests.add(new MyDecimalTestStruct(".12345000098765", "0.12345000098765", 14, 14));
    tests.add(
        new MyDecimalTestStruct("-.000000012345000098765", "-0.000000012345000098765", 21, 21));
    tests.add(
        new MyDecimalTestStruct("-.000000012345000098765", "-0.000000012345000098765", 2, 21));
    tests.add(
        new MyDecimalTestStruct("-.000000012345000098765", "-0.000000012345000098765", 21, 2));
    tests.add(new MyDecimalTestStruct("0000000.001", "0.001", 3, 3));
    tests.add(new MyDecimalTestStruct("1234500009876.5", "1234500009876.5", 14, 1));
    for (MyDecimalTestStruct t : tests) {
      MyDecimal dec = new MyDecimal();
      dec.fromString(t.in);
      assertEquals(t.out, dec.toString());
    }
  }

  @Test
  public void readWordTest() {
    assertEquals(-6, MyDecimal.readWord(new int[] {250}, 1, 0));
    assertEquals(50, MyDecimal.readWord(new int[] {50}, 1, 0));

    assertEquals(-1286, MyDecimal.readWord(new int[] {250, 250}, 2, 0));
    assertEquals(12850, MyDecimal.readWord(new int[] {50, 50}, 2, 0));

    assertEquals(-328966, MyDecimal.readWord(new int[] {250, 250, 250}, 3, 0));
    assertEquals(3289650, MyDecimal.readWord(new int[] {50, 50, 50}, 3, 0));

    assertEquals(-84215046, MyDecimal.readWord(new int[] {250, 250, 250, 250}, 4, 0));
    assertEquals(842150450, MyDecimal.readWord(new int[] {50, 50, 50, 50}, 4, 0));
  }

  @Test
  public void toBinFromBinTest() {
    List<MyDecimalTestStruct> tests = new ArrayList<>();
    String decValStr = "11111111111111111111111111111111111.111111111111111111111111111111";
    tests.add(new MyDecimalTestStruct(decValStr, decValStr, 65, 30));
    tests.add(new MyDecimalTestStruct("12345000098765", "12345000098765", 14, 0));
    tests.add(new MyDecimalTestStruct("-10.55", "-10.55", 4, 2));
    tests.add(new MyDecimalTestStruct("12345", "12345", 5, 0));
    tests.add(new MyDecimalTestStruct("-12345", "-12345", 5, 0));
    tests.add(new MyDecimalTestStruct("0000000.001", "0.001", 3, 3));
    tests.add(new MyDecimalTestStruct("0.00012345000098765", "0.00012345000098765", 17, 17));
    tests.add(new MyDecimalTestStruct("-0.00012345000098765", "-0.00012345000098765", 17, 17));
    tests.add(new MyDecimalTestStruct("0", "0", 1, 0));
    for (MyDecimalTestStruct a : tests) {
      MyDecimal dec = new MyDecimal();
      dec.fromString(a.in);
      assertEquals(a.out, dec.toString());
      int[] bin = dec.toBin(a.precision, a.frac);
      dec.clear();
      dec.fromBin(a.precision, a.frac, bin);
      assertEquals(a.out, dec.toString());
    }
  }

  @Test
  public void toBinTest() {
    MyDecimal dec = new MyDecimal();
    dec.fromDecimal(-1234567890.1234);
    int[] data = dec.toBin(dec.precision(), dec.frac());
    int[] expected =
        new int[] {
          0x7E, 0xF2, 0x04, 0xC7, 0x2D, 0xFB, 0x2D,
        };
    assertArrayEquals(expected, data);
  }

  // MyDecimalTestStruct is only used for simplifying testing.
  private static class MyDecimalTestStruct {
    String in;
    String out;
    int precision;
    int frac;

    MyDecimalTestStruct(String in, String out, int precision, int frac) {
      this.in = in;
      this.out = out;
      this.precision = precision;
      this.frac = frac;
    }
  }
}
