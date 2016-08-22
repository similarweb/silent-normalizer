What is a region normalizer
===========================

A region normalizer is a strategy to control the regions’ sizes when the table
changes dynamically. Once in awhile the HBase master asks the normalizer to
calculate normalization plans and the normalizer responds with the list of regions
which have to be splitted and/or merged. The HBase master then executes these splits
and merges when it has opportunity to do so.

Reference: SimpleRegionNormalizer
---------------------------------

HBase comes with one example of region normalizer, called SimpleRegionNormalizer.
The goal of this normalizer is to calculate an average region size per table, and
then to find outliers. Regions which are more than twice larger than the average are
planned to be split and the small regions are merged if their merged size is still
less than the average. By this method, the regions’ sizes become closer to the average.
The code of this normalizer is available here:

https://github.com/apache/hbase/blob/master/hbase-server/src/main/java/org/apache/hadoop/hbase/master/normalizer/SimpleRegionNormalizer.java

Goals of our normalizer
=======================

In our case, the regions already had approximately equal size already. So the goal was to
decrease the number of them, starting from the least used ones. Fortunately, the HBase
provides the request count for each region. When I checked, I found that many regions on
our servers had this count on zero, and many other had low numbers. This parameter is
configurable, but first of all we could start with regions which had zero request count.
The idea was to merge regions which are not used or almost not used until they become too
big or until the table has too few regions. As I told earlier, we started with 200-300
regions per table. So the limit of 20-30 regions per table will mean 10 times less regions.

This is the full list of configuration parameters:

* minRegionsPerTable (default 20)
* maxRegionSize (in megabytes, default 3072 for 3GB)
* maxResults (default 10)
* maxRequestCount (default 0)
