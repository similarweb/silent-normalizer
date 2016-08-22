/**
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 *   Copyright (c) 2016, SimilarWeb LTD.
 *   All rights reserved.
 *
 *   Redistribution and use in source and binary forms, with or without
 *   modification, are permitted provided that the following conditions are met:
 *
 *  1. Redistributions of source code must retain the above copyright notice, this
 *      list of conditions and the following disclaimer.
 *
 *   2. Redistributions in binary form must reproduce the above copyright notice,
 *     this list of conditions and the following disclaimer in the documentation
 *     and/or other materials provided with the distribution.
 *
 *   THIS SOFTWARE IS PROVIDED BY SimilarWeb ``AS IS'' AND ANY EXPRESS OR IMPLIED
 *   WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF
 *   MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO
 *   EVENT SHALL SimilarWeb OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT,
 *
 *   LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA,
 *   OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF
 *   LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING
 *   NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE,
 *   EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 *
 *   The views and conclusions contained in the software and documentation are
 *   those of the authors and should not be interpreted as representing official
 *   policies, either expressed or implied, of SimilarWeb.
*/

package com.similarweb.hbase;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.master.MasterServices;
import org.apache.hadoop.hbase.master.normalizer.MergeNormalizationPlan;
import org.apache.hadoop.hbase.master.normalizer.NormalizationPlan;
import org.apache.hadoop.hbase.master.normalizer.RegionNormalizer;

import java.util.List;

/**
 * Simple implementation of region normalizer.
 *
 * Logic in use:
 *
 *  <ol>
 *  <li> get all regions of a given table
 *  <li> get avg size S of each region (by total size of store files reported in RegionLoad)
 *  <li> If biggest region is bigger than S * 2, it is kindly requested to split,
 *    and normalization stops
 *  <li> Otherwise, two smallest region R1 and its smallest neighbor R2 are kindly requested
 *    to merge, if R1 + R1 &lt;  S, and normalization stops
 *  <li> Otherwise, no action is performed
 * </ol>
 * <p>
 * Region sizes are coarse and approximate on the order of megabytes. Additionally,
 * "empty" regions (less than 1MB, with the previous note) are not merged away. This
 * is by design to prevent normalization from undoing the pre-splitting of a table.
 */
@InterfaceAudience.Private
public class SilentRegionNormalizer implements RegionNormalizer {

  private static final Log LOG = LogFactory.getLog(SilentRegionNormalizer.class);

  public static final String MIN_REGION_COUNT = "similarweb.normalizer.min_region_count";
  private static final int DEFAULT_MIN_REGION_COUNT = 30;
  public static final String MAX_MERGED_SIZE = "similarweb.normalizer.max_merged_size";
  private static final int DEFAULT_MAX_MERGED_SIZE = 3072; //3 gigabyte
  public static final String MAX_REQUEST_COUNT = "similarweb.normalizer.max_request_count";
  private static final int DEFAULT_MAX_REQUEST_COUNT = 0;
  public static final String MAX_RESULTS = "similarweb.normalizer.max_results";
  private static final int DEFAULT_MAX_RESULTS = 10;

  private MasterServices masterServices;
  private RegionMergeConfig mergeConfig;

  /**
   * Set the master service.
   * @param masterServices inject instance of MasterServices
   */
  @Override
  public void setMasterServices(MasterServices masterServices) {
    this.masterServices = masterServices;

    Configuration serverConfig = masterServices.getConfiguration();

    mergeConfig = RegionMergeConfig.create()
            .withMaxRegionSize(serverConfig.getInt(MAX_MERGED_SIZE, DEFAULT_MAX_MERGED_SIZE))
            .withMaxRequestCount(serverConfig.getInt(MAX_REQUEST_COUNT, DEFAULT_MAX_REQUEST_COUNT))
            .withMaxResults(serverConfig.getInt(MAX_RESULTS, DEFAULT_MAX_RESULTS))
            .withMinRegionsCount(serverConfig.getInt(MIN_REGION_COUNT, DEFAULT_MIN_REGION_COUNT))
            .build();

    LOG.info("Read configuration: [max size:"+mergeConfig.getMaxRegionSize()
            + ", max requests:"+mergeConfig.getMaxRequestCount()
            +", max results:"+mergeConfig.getMaxResults()
            +", min regions:"+mergeConfig.getMinRegionsCount()
            +"]");
  }

  /**
   * Computes next most "urgent" normalization action on the table.
   * Action may be either a split, or a merge, or no action.
   *
   * @param table table to normalize
   * @return normalization plan to execute
   */
  @Override
  public List<NormalizationPlan> computePlanForTable(TableName table) throws HBaseIOException {
    if (table == null || table.isSystemTable()) {
      LOG.debug("Normalization of system table " + table + " isn't allowed");
      return null;
    }

    List<HRegionInfo> tableRegions = masterServices.getAssignmentManager().getRegionStates().
            getRegionsOfTable(table);


    LOG.info("Computing normalization plan for table: " + table +
            ", number of regions: " + tableRegions.size());

    List<NormalizationPlan> plans =
            tableRegions.stream()
                    .map(this::toRegionInfo)
                    .collect(RegionPairsCollection.createCollector(mergeConfig))
                    .map(this::toNormalizationPlan)
                    .collect(java.util.stream.Collectors.toList());

    if (plans.isEmpty()) {
      LOG.info("No normalization needed, regions look good for table: " + table);
      return null;
    } else {
      LOG.info("Found "+plans.size()+" pairs of regions to merge for table " + table);
      return plans;
    }
  }

  private NormalizationPlan toNormalizationPlan(RegionPair<HRegionInfo> ri) {
    return new MergeNormalizationPlan(ri.handle, ri.nextHandle);
  }

  private RegionPair<HRegionInfo> toRegionInfo(HRegionInfo hri) {
    try {
      ServerName sn = masterServices.getAssignmentManager().getRegionStates().
              getRegionServerOfRegion(hri);
      RegionLoad regionLoad = masterServices.getServerManager().getLoad(sn).
              getRegionsLoad().get(hri.getRegionName());
      return new RegionPair(hri, regionLoad.getStorefileSizeMB(), regionLoad.getRequestsCount());
    } catch (Exception e) {
      LOG.error(e);
      return null;
    }
  }
}
