package com.github.leafty.hudi;

import com.beust.jcommander.internal.Lists;
import com.uber.hoodie.hive.PartitionValueExtractor;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public class SlashToDashPartitionValueExtractor implements PartitionValueExtractor {

  @Override
  public List<String> extractPartitionValuesInPath(String partitionPath) {
    // partition path is expected to be in this format xx/yy/tt
    String[] splits = partitionPath.split("/");

    String collect = Arrays.stream(splits).collect(Collectors.joining("-")).replace("/", "");
    return Lists.newArrayList(collect);
  }
}

