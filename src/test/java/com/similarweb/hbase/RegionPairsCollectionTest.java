/**
 *    Copyright (c) 2016, SimilarWeb LTD.
 *    All rights reserved.
 *
 *    Redistribution and use in source and binary forms, with or without
 *    modification, are permitted provided that the following conditions are met:
 *
 *    1. Redistributions of source code must retain the above copyright notice, this
 *       list of conditions and the following disclaimer.
 *
 *    2. Redistributions in binary form must reproduce the above copyright notice,
 *       this list of conditions and the following disclaimer in the documentation
 *       and/or other materials provided with the distribution.
 *
 *    THIS SOFTWARE IS PROVIDED BY SimilarWeb ``AS IS'' AND ANY EXPRESS OR IMPLIED
 *    WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF
 *    MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO
 *    EVENT SHALL SimilarWeb OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT,
 *    INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
 *    LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA,
 *    OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF
 *    LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING
 *    NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE,
 *    EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 *
 *    The views and conclusions contained in the software and documentation are
 *    those of the authors and should not be interpreted as representing official
 *    policies, either expressed or implied, of SimilarWeb.
*/

package com.similarweb.hbase;

import org.junit.Test;

import java.util.Arrays;

import static org.hamcrest.Matchers.*;
import static org.junit.Assert.*;

/**
 * Created by andrews on 09/08/16.
 */
public class RegionPairsCollectionTest {
    RegionPair<String>[] inputs = new RegionPair[]{
            new RegionPair("region1", 10, 0),
            null,
            new RegionPair("region2", 5, 0),
            new RegionPair("region3", 5, 0),
            new RegionPair("region4", 2, 10),
            new RegionPair("region5", 2, 0),
            null,
            new RegionPair("region6", 1, 0)
    };

    @Test
    public void testMinSize() {
        //parallel here is optional, but nice to have
        assertThat((Arrays.stream(inputs).parallel())
                .collect(
                        RegionPairsCollection.createCollector(RegionMergeConfig.create()
                                .withMaxRegionSize(100)
                                .withMaxResults(100)
                                .withMinRegionsCount(5)
                                .withMinRegionsCount(100)
                                .build()
                        )
                )
                .toArray(), emptyArray());

        assertThat(Arrays.stream(inputs)
                .collect(
                        RegionPairsCollection.createCollector(RegionMergeConfig.create()
                                .withMaxRegionSize(100)
                                .withMaxResults(100)
                                .withMinRegionsCount(104)
                                .withMaxRequestCount(100)
                                .build()
                        )
                )
                .toArray(), emptyArray());
    }

    private String ri2string(RegionPair<String> ri) {
        return ri.handle + "|" + ri.nextHandle;
    }

    @Test
    public void testFindOne() {
        //parallel here is optional but nice to have
        assertThat(Arrays.stream(inputs).parallel()
                .collect(
                        RegionPairsCollection.createCollector(RegionMergeConfig.create()
                                .withMaxRegionSize(100)
                                .withMaxResults(1)
                                .withMinRegionsCount(1)
                                .withMaxRequestCount(100)
                                .build()
                        )
                )
                .map(this::ri2string)
                .collect(java.util.stream.Collectors.toList()), contains("region4|region5"));
    }

    @Test
    public void testRequestCount() {
        //parallel here is optional but nice to have
        assertThat(Arrays.stream(inputs).parallel()
                .collect(
                        RegionPairsCollection.createCollector(RegionMergeConfig.create()
                                .withMaxRegionSize(100)
                                .withMaxResults(1)
                                .withMinRegionsCount(1)
                                .withMaxRequestCount(0)
                                .build()
                        )
                )
                .map(this::ri2string)
                .collect(java.util.stream.Collectors.toList()), contains("region2|region3"));
    }

    @Test
    public void testFindTwo() {
        //this test will fail without parallel
        //as the regions come in descending order, every next region will replace the previously found one
        assertThat(Arrays.stream(inputs).parallel()
                .collect(
                        RegionPairsCollection.createCollector(RegionMergeConfig.create()
                                .withMaxRegionSize(100)
                                .withMaxResults(2)
                                .withMinRegionsCount(1)
                                .withMaxRequestCount(100)
                                .build()
                        )
                )
                .map(this::ri2string)
                .collect(java.util.stream.Collectors.toList()), contains("region4|region5","region2|region3"));
    }

}