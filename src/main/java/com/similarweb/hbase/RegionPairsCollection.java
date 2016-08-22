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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.*;
import java.util.stream.Collector;
import java.util.stream.Stream;

/**
 * Created by andrews on 10/08/16.
 */
class RegionPairsCollection<T> {

    private static final Log LOG = LogFactory.getLog(RegionPairsCollection.class);

    public static <T> Collector<RegionPair<T>, RegionPairsCollection<T>, Stream<RegionPair<T>>> createCollector(RegionMergeConfig config) {
        return new CollectorImpl<>(
                () -> new RegionPairsCollection<T>(config),
                RegionPairsCollection::add,
                RegionPairsCollection::addAll,
                RegionPairsCollection::result,
                Collections.emptySet()
        );
    }

    RegionMergeConfig config;

    boolean isEmpty = true;
    int processedCount = 0;
    SortedSet<RegionPair<T>> pairSortedSet
            = new TreeSet<>((ri1, ri2) -> Long.compare(ri1.mergedSize, ri2.mergedSize));

    RegionPair<T> last = null;
    RegionPair<T> first = null;

    RegionPairsCollection(RegionMergeConfig config) {
        this.config = config;
    }

    RegionPairsCollection(RegionMergeConfig config, RegionPair<T> ri) {
        this(config);
        first = ri;
        last = ri;
        isEmpty = false;
        if (ri != null) {
            processedCount = 1;
        }
    }

    void add(RegionPair<T> ri) {
        addAll(new RegionPairsCollection<>(config, ri));
    }

    RegionPairsCollection<T> addAll(RegionPairsCollection<T> other) {
        if (other.isEmpty) {
            return this; //other is Unity
        }
        if (isEmpty) {
            //this is Unity, just become the other
            processedCount = other.processedCount;
            isEmpty = other.isEmpty;
            pairSortedSet = other.pairSortedSet;
            first = other.first;
            last = other.last;
            return this;
        }

        // pair between our last and next first if both are not null
        if (last != null && other.first != null) {
            last.nextHandle = other.first.handle;
            last.mergedSize = last.size + other.first.size;
            if (last.requestCount > config.getMaxRequestCount()) {
                LOG.debug("Skipping region "+last.handle+" for too high request count:"+last.requestCount);
            } else if (other.first.requestCount > config.getMaxRequestCount()) {
                LOG.debug("Skipping region "+other.first.handle+" for too high request count:"+other.first.requestCount);
            } else if (last.mergedSize >= config.getMaxRegionSize()) {
                LOG.debug("Skipping region "+last.handle+" for too large region size:"+last.mergedSize);
            } else {
                LOG.debug("Add region "+last.handle+" with size "+last.mergedSize);
                pairSortedSet.add(last);
            }
        }
        last = other.last;

        pairSortedSet.addAll(other.pairSortedSet);

        processedCount += other.processedCount;
        return this;
    }


    Stream<RegionPair<T>> result() {
        if (processedCount <= config.getMinRegionsCount()) {
            LOG.info("returning empty collection because of not enough inputs");
            return Stream.empty();
        }
        int maxReturnSize = Integer.min(pairSortedSet.size(), config.getMaxResults());
        LOG.info("Returning maximum "+maxReturnSize+" out of "+ pairSortedSet.size()+" results. Final result size may be less because of prohibited neighbours.");

        final Set<T> prohibited = new HashSet<>();
        //TODO: here there is a very dirty tricks: we change the prohibited set as a side effect
        //can be fixed by using a proper accumulator with permanent collections
        return pairSortedSet.stream()
                .filter(ri -> {
                    if(prohibited.contains(ri.handle) || prohibited.contains(ri.nextHandle)) {
                        LOG.debug("Skipping region "+ri.handle+" for being a prohibited neighbour");
                        return false;
                    }
                    prohibited.add(ri.handle);
                    prohibited.add(ri.nextHandle);
                    return true;
                })
                .limit(maxReturnSize);
    }
}
