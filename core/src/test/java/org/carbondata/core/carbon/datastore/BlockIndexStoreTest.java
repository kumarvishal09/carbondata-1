package org.carbondata.core.carbon.datastore;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.carbondata.core.carbon.AbsoluteTableIdentifier;
import org.carbondata.core.carbon.CarbonTableIdentifier;
import org.carbondata.core.carbon.datastore.block.AbstractIndex;
import org.carbondata.core.carbon.datastore.block.TableBlockInfo;
import org.carbondata.core.carbon.datastore.exception.IndexBuilderException;
import org.carbondata.core.carbon.querystatistics.QueryStatisticsRecorder;

import junit.framework.TestCase;

import org.junit.BeforeClass;
import org.junit.Test;

public class BlockIndexStoreTest extends TestCase {

  private BlockIndexStore indexStore;

  @BeforeClass public void setUp() {
    indexStore = BlockIndexStore.getInstance();
  }

  @Test public void testloadAndGetTaskIdToSegmentsMapForSingleSegment() throws IOException {
    String canonicalPath =
        new File(this.getClass().getResource("/").getPath() + "/../../").getCanonicalPath();
    File file = new File(canonicalPath + "/src/test/resources/part-0-0-1466029397000.carbondata");
    TableBlockInfo info =
        new TableBlockInfo(file.getAbsolutePath(), 0, "0", new String[] { "loclhost" },
            file.length());
    CarbonTableIdentifier carbonTableIdentifier = new CarbonTableIdentifier("default", "t3", "1");
    AbsoluteTableIdentifier absoluteTableIdentifier =
        new AbsoluteTableIdentifier("/src/test/resources", carbonTableIdentifier);
    QueryStatisticsRecorder recoStatisticsRecorder = new QueryStatisticsRecorder("TestQuery");
    try {
      List<AbstractIndex> loadAndGetBlocks = indexStore
          .loadAndGetBlocks(Arrays.asList(new TableBlockInfo[] { info }), absoluteTableIdentifier,recoStatisticsRecorder);
      assertTrue(loadAndGetBlocks.size() == 1);
    } catch (IndexBuilderException e) {
      assertTrue(false);
    }
    indexStore.clear(absoluteTableIdentifier);
  }

  @Test public void testloadAndGetTaskIdToSegmentsMapForSameBlockLoadedConcurrently()
      throws IOException {
    String canonicalPath =
        new File(this.getClass().getResource("/").getPath() + "/../../").getCanonicalPath();
    File file = new File(canonicalPath + "/src/test/resources/part-0-0-1466029397000.carbondata");
    TableBlockInfo info =
        new TableBlockInfo(file.getAbsolutePath(), 0, "0", new String[] { "loclhost" },
            file.length());
    TableBlockInfo info1 =
        new TableBlockInfo(file.getAbsolutePath(), 0, "0", new String[] { "loclhost" },
            file.length());

    TableBlockInfo info2 =
        new TableBlockInfo(file.getAbsolutePath(), 0, "1", new String[] { "loclhost" },
            file.length());
    TableBlockInfo info3 =
        new TableBlockInfo(file.getAbsolutePath(), 0, "1", new String[] { "loclhost" },
            file.length());
    TableBlockInfo info4 =
        new TableBlockInfo(file.getAbsolutePath(), 0, "1", new String[] { "loclhost" },
            file.length());

    CarbonTableIdentifier carbonTableIdentifier = new CarbonTableIdentifier("default", "t3", "1");
    AbsoluteTableIdentifier absoluteTableIdentifier =
        new AbsoluteTableIdentifier("/src/test/resources", carbonTableIdentifier);
    ExecutorService executor = Executors.newFixedThreadPool(3);
    QueryStatisticsRecorder recoStatisticsRecorder = new QueryStatisticsRecorder("TestQuery");
    executor.submit(new BlockLoaderThread(Arrays.asList(new TableBlockInfo[] { info, info1 }),
        absoluteTableIdentifier,recoStatisticsRecorder));
    QueryStatisticsRecorder recoStatisticsRecorder1 = new QueryStatisticsRecorder("TestQuery1");
    executor.submit(
        new BlockLoaderThread(Arrays.asList(new TableBlockInfo[] { info2, info3, info4 }),
            absoluteTableIdentifier,recoStatisticsRecorder1));
    QueryStatisticsRecorder recoStatisticsRecorder2 = new QueryStatisticsRecorder("TestQuery3");
    executor.submit(new BlockLoaderThread(Arrays.asList(new TableBlockInfo[] { info, info1 }),
        absoluteTableIdentifier,recoStatisticsRecorder1));
    QueryStatisticsRecorder recoStatisticsRecorder3 = new QueryStatisticsRecorder("TestQuery4");
    executor.submit(
        new BlockLoaderThread(Arrays.asList(new TableBlockInfo[] { info2, info3, info4 }),
            absoluteTableIdentifier,recoStatisticsRecorder3));
    executor.shutdown();
    try {
      executor.awaitTermination(1, TimeUnit.DAYS);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
    QueryStatisticsRecorder recoStatisticsRecorder4 = new QueryStatisticsRecorder("TestQuery5");
    try {
      List<AbstractIndex> loadAndGetBlocks = indexStore.loadAndGetBlocks(
          Arrays.asList(new TableBlockInfo[] { info, info1, info2, info3, info4 }),
          absoluteTableIdentifier,recoStatisticsRecorder4);
      assertTrue(loadAndGetBlocks.size() == 5);
    } catch (IndexBuilderException e) {
      assertTrue(false);
    }
    indexStore.clear(absoluteTableIdentifier);
  }

  @Test public void testloadAndGetTaskIdToSegmentsMapForDifferentSegmentLoadedConcurrently()
      throws IOException {
    String canonicalPath =
        new File(this.getClass().getResource("/").getPath() + "/../../").getCanonicalPath();
    File file = new File(canonicalPath + "/src/test/resources/part-0-0-1466029397000.carbondata");
    TableBlockInfo info =
        new TableBlockInfo(file.getAbsolutePath(), 0, "0", new String[] { "loclhost" },
            file.length());
    TableBlockInfo info1 =
        new TableBlockInfo(file.getAbsolutePath(), 0, "0", new String[] { "loclhost" },
            file.length());

    TableBlockInfo info2 =
        new TableBlockInfo(file.getAbsolutePath(), 0, "1", new String[] { "loclhost" },
            file.length());
    TableBlockInfo info3 =
        new TableBlockInfo(file.getAbsolutePath(), 0, "1", new String[] { "loclhost" },
            file.length());
    TableBlockInfo info4 =
        new TableBlockInfo(file.getAbsolutePath(), 0, "1", new String[] { "loclhost" },
            file.length());

    TableBlockInfo info5 =
        new TableBlockInfo(file.getAbsolutePath(), 0, "2", new String[] { "loclhost" },
            file.length());
    TableBlockInfo info6 =
        new TableBlockInfo(file.getAbsolutePath(), 0, "2", new String[] { "loclhost" },
            file.length());

    TableBlockInfo info7 =
        new TableBlockInfo(file.getAbsolutePath(), 0, "3", new String[] { "loclhost" },
            file.length());

    CarbonTableIdentifier carbonTableIdentifier = new CarbonTableIdentifier("default", "t3", "1");
    AbsoluteTableIdentifier absoluteTableIdentifier =
        new AbsoluteTableIdentifier("/src/test/resources", carbonTableIdentifier);
    ExecutorService executor = Executors.newFixedThreadPool(3);
    QueryStatisticsRecorder recoStatisticsRecorder1 = new QueryStatisticsRecorder("TestQuery1");
    executor.submit(new BlockLoaderThread(Arrays.asList(new TableBlockInfo[] { info, info1 }),
        absoluteTableIdentifier,recoStatisticsRecorder1));
    QueryStatisticsRecorder recoStatisticsRecorder2 = new QueryStatisticsRecorder("TestQuery2");
    executor.submit(
        new BlockLoaderThread(Arrays.asList(new TableBlockInfo[] { info2, info3, info4 }),
            absoluteTableIdentifier,recoStatisticsRecorder2));
    QueryStatisticsRecorder recoStatisticsRecorder3 = new QueryStatisticsRecorder("TestQuery3");
    executor.submit(new BlockLoaderThread(Arrays.asList(new TableBlockInfo[] { info5, info6 }),
        absoluteTableIdentifier,recoStatisticsRecorder3));
    QueryStatisticsRecorder recoStatisticsRecorder4 = new QueryStatisticsRecorder("TestQuery4");
    executor.submit(new BlockLoaderThread(Arrays.asList(new TableBlockInfo[] { info7 }),
        absoluteTableIdentifier,recoStatisticsRecorder4));

    executor.shutdown();
    try {
      executor.awaitTermination(1, TimeUnit.DAYS);
    } catch (InterruptedException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
    QueryStatisticsRecorder recoStatisticsRecorder = new QueryStatisticsRecorder("TestQuery");
    try {
      List<AbstractIndex> loadAndGetBlocks = indexStore.loadAndGetBlocks(Arrays
              .asList(new TableBlockInfo[] { info, info1, info2, info3, info4, info5, info6, info7 }),
          absoluteTableIdentifier,recoStatisticsRecorder);
      assertTrue(loadAndGetBlocks.size() == 8);
    } catch (IndexBuilderException e) {
      assertTrue(false);
    }
    indexStore.clear(absoluteTableIdentifier);
  }

  private class BlockLoaderThread implements Callable<Void> {
    private List<TableBlockInfo> tableBlockInfoList;
    private AbsoluteTableIdentifier absoluteTableIdentifier;
    private QueryStatisticsRecorder recoQueryStatisticsRecorder;
    public BlockLoaderThread(List<TableBlockInfo> tableBlockInfoList,
        AbsoluteTableIdentifier absoluteTableIdentifier,QueryStatisticsRecorder recoQueryStatisticsRecorder) {
      // TODO Auto-generated constructor stub
      this.tableBlockInfoList = tableBlockInfoList;
      this.absoluteTableIdentifier = absoluteTableIdentifier;
      this.recoQueryStatisticsRecorder=recoQueryStatisticsRecorder;
    }

    @Override public Void call() throws Exception {
      indexStore.loadAndGetBlocks(tableBlockInfoList, absoluteTableIdentifier,recoQueryStatisticsRecorder);
      return null;
    }

  }
}
