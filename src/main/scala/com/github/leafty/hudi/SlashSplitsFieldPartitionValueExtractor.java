package com.github.leafty.hudi;

import com.beust.jcommander.internal.Lists;
import com.uber.hoodie.hive.PartitionValueExtractor;
import java.util.List;

public class SlashSplitsFieldPartitionValueExtractor  implements  PartitionValueExtractor {

  public SlashSplitsFieldPartitionValueExtractor() {
  }

  public List<String> extractPartitionValuesInPath(String partitionPath) {
    return Lists.newArrayList(partitionPath.split("/"));
  }

}
