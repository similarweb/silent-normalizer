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

/**
 * Created by andrews on 10/08/16.
 */
public class RegionMergeConfig {

    private int minRegionsCount = 20;
    private int maxRegionSize = 3072;
    private int maxResults = 1;
    private long maxRequestCount = 0;

    public int getMinRegionsCount() {
        return minRegionsCount;
    }

    public int getMaxRegionSize() {
        return maxRegionSize;
    }

    public int getMaxResults() {
        return maxResults;
    }

    public long getMaxRequestCount() {
        return maxRequestCount;
    }

    public class Builder {
        public Builder withMinRegionsCount(int minRegionsCount) {
            RegionMergeConfig.this.minRegionsCount = minRegionsCount;
            return this;
        }
        public Builder withMaxRegionSize(int maxRegionsSize) {
            RegionMergeConfig.this.maxRegionSize = maxRegionsSize;
            return this;
        }
        public Builder withMaxResults(int maxResults) {
            RegionMergeConfig.this.maxResults = maxResults;
            return this;
        }
        public Builder withMaxRequestCount (int maxRequestCount) {
            RegionMergeConfig.this.maxRequestCount = maxRequestCount;
            return this;
        }
        public RegionMergeConfig build() {
            return RegionMergeConfig.this;
        }
    }

    private RegionMergeConfig() {
    }

    public static Builder create() {
        return new RegionMergeConfig().new Builder();
    }

}
