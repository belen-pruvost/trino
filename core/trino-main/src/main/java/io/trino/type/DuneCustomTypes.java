/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.trino.type;

import java.math.BigInteger;

public class DuneCustomTypes
{
    public static class Int256
    {
        public static final String INT256 = "int256";
        public static final BigInteger MAX_VALUE = BigInteger.TWO.pow(255).subtract(BigInteger.ONE);
        public static final BigInteger MIN_VALUE = BigInteger.TWO.pow(255).negate();
    }

    public static class Uint256
    {
        public static final String UINT256 = "uint256";
        public static final BigInteger MAX_VALUE = BigInteger.TWO.pow(256).subtract(BigInteger.ONE);
        public static final BigInteger MIN_VALUE = BigInteger.ZERO;
    }
}
