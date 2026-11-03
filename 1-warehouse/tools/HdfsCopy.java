// Copy thu muc Parquet tu dia Windows len HDFS bang client HDFS goc (khong qua WebHDFS).
// Chep tung byte giuyen ven -> manifest kiem toan whole cua slice 2 van dung tren HDFS.
// Chay lai duoc de hoi tiep (resume): file nao da co tren HDFS cung kich thuoc thi bo qua.
//
// Bien dich (can JDK 17 + cac jar Hadoop ma PySpark 4.0.1 mang san):
//   & "C:\Program Files\Eclipse Adoptium\jdk-17.0.15.6-hotspot\bin\javac" `
//       -cp "D:\BIGDATA\2-processing\.venv\Lib\site-packages\pyspark\jars\*" `
//       -d D:\BIGDATA\1-warehouse\tools\classes HdfsCopy.java
// Chay:
//   & "C:\Program Files\Eclipse Adoptium\jdk-17.0.15.6-hotspot\bin\java" `
//       "-Dhadoop.home.dir=D:\hadoop" `
//       -cp "D:\BIGDATA\2-processing\.venv\Lib\site-packages\pyspark\jars\*;D:\BIGDATA\1-warehouse\tools\classes" `
//       HdfsCopy D:\BIGDATA\2-processing\data\clean\trips /data/nyc_taxi/clean/trips hdfs://<IP_may>:8020 4
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

public class HdfsCopy {

  static class Item {
    final File src;
    final String rel;

    Item(File src, String rel) {
      this.src = src;
      this.rel = rel;
    }
  }

  public static void main(String[] args) throws Exception {
    if (args.length < 3) {
      System.out.println("Cach dung: HdfsCopy <localRoot> <hdfsPath> <hdfs://host:8020> [threads]");
      System.exit(2);
    }
    File localRoot = new File(args[0]);
    String hdfsRoot = args[1];
    String nn = args[2];
    int threads = args.length > 3 ? Integer.parseInt(args[3]) : 4;
    // Tham so "tlc": doi ten duong dan tu goc TLC "yellow_tripdata_YYYY-MM.parquet"
    // thanh phan vuong Hive "year=YYYY/month=MM/part-...". Chi doi duong dan,
    // noi dung file van chep nguyen byte.
    boolean tlcLayout = args.length > 4 && "tlc".equals(args[4]);
    if (!localRoot.isDirectory()) {
      System.out.println("[FAIL] Thu muc local khong ton tai: " + localRoot.getAbsolutePath());
      System.exit(1);
    }

    Configuration c = new Configuration();
    c.set("fs.defaultFS", nn);
    c.set("dfs.replication", "1");
    // Client ben ngoai container phai theo hostname cua DataNode, khong theo IP noi bo Docker.
    c.setBoolean("dfs.client.use.datanode.hostname", true);
    FileSystem fs = FileSystem.get(c);

    List<Item> items = new ArrayList<>();
    long localBytes = 0;
    List<File> queue = new ArrayList<>();
    queue.add(localRoot);
    for (int i = 0; i < queue.size(); i++) {
      File dir = queue.get(i);
      File[] children = dir.listFiles();
      if (children == null) continue;
      for (File f : children) {
        if (f.isDirectory()) {
          queue.add(f);
        } else if (!f.getName().startsWith(".")) {
          // Loai bo file .crc / _SUCCESS an cua Windows va Spark.
          String rel = f.getAbsolutePath().substring(localRoot.getAbsolutePath().length() + 1)
              .replace(File.separatorChar, '/');
          if (tlcLayout) rel = tlcPartition(rel);
          items.add(new Item(f, rel));
          localBytes += f.length();
        }
      }
    }

    System.out.println("[scan] " + items.size() + " tep, " + mib(localBytes) + " MiB can len "
        + hdfsRoot + "  (" + threads + " luong)");

    AtomicLong copied = new AtomicLong();
    AtomicLong copiedBytes = new AtomicLong();
    AtomicLong skipped = new AtomicLong();
    AtomicLong done = new AtomicLong();
    List<String> failures = java.util.Collections.synchronizedList(new ArrayList<>());
    long t0 = System.currentTimeMillis();
    ExecutorService pool = Executors.newFixedThreadPool(threads);
    final FileSystem dfs = fs;
    final String root = hdfsRoot;

    for (Item it : items) {
      pool.submit(() -> {
        Path dst = new Path(root + "/" + it.rel);
        try {
          if (dfs.exists(dst) && dfs.getFileStatus(dst).getLen() == it.src.length()) {
            skipped.incrementAndGet();
          } else {
            dfs.mkdirs(dst.getParent());
            long n = stream(dfs, it, dst);
            copiedBytes.addAndGet(n);
            copied.incrementAndGet();
          }
        } catch (Throwable e) {
          failures.add(it.rel + " -> " + e.getClass().getSimpleName() + ": " + e.getMessage());
          System.out.println("[FAIL file] " + it.rel + ": " + e);
        }
        // Bam tien do moi 20 tep: du de quan sat ma khong tran log.
        long k = done.incrementAndGet();
        if (k % 20 == 0 || k == items.size()) {
          System.out.printf("[tien do] %d/%d tep  %s MiB da sang  %d bo qua (trung kich thuoc)%n",
              k, items.size(), fmt(copiedBytes.get()), skipped.get());
        }
      });
    }
    pool.shutdown();
    pool.awaitTermination(12, TimeUnit.HOURS);

    long ms = System.currentTimeMillis() - t0;
    double sec = Math.max(ms / 1000.0, 0.001);
    System.out.printf("[summary] copy=%d skip=%d  %s MiB trong %.0f s = %.1f MiB/s%n",
        copied.get(), skipped.get(), fmt(copiedBytes.get()), sec,
        copiedBytes.get() / 1048576.0 / sec);
    System.out.println("[summary] that bai: " + failures.size());
    for (String f : failures) System.out.println("   - " + f);

    // Nghiem thu: so byte thuc te nam tren HDFS duoi cung goc.
    long hdfsBytes = 0;
    int hdfsFiles = 0;
    List<Path> stack = new ArrayList<>();
    stack.add(new Path(root));
    for (int i = 0; i < stack.size(); i++) {
      var st = dfs.listStatus(stack.get(i));
      if (st == null) continue;
      for (var s : st) {
        if (s.isDirectory()) stack.add(s.getPath());
        else if (!s.getPath().getName().startsWith(".")) {
          hdfsBytes += s.getLen();
          hdfsFiles++;
        }
      }
    }
    System.out.println("[verify] tren dia local = " + items.size() + " tep / " + localBytes
        + " byte;  tren HDFS = " + hdfsFiles + " tep / " + hdfsBytes + " byte");
    boolean ok = failures.isEmpty() && hdfsBytes == localBytes && hdfsFiles == items.size();
    System.out.println(ok ? "PASS" : "FAIL");
    System.exit(ok ? 0 : 1);
  }

  // "yellow_tripdata_2024-01.parquet" -> "year=2024/month=01/part-2024-01.parquet".
  // File khong dat theo khuon nay (vi du taxi_zone_lookup.csv) giu nguyen duong dan.
  private static String tlcPartition(String rel) {
    String name = rel.contains("/") ? rel.substring(rel.lastIndexOf('/') + 1) : rel;
    java.util.regex.Matcher m =
        java.util.regex.Pattern.compile("^(.*)_(\\d{4})-(\\d{2})\\.parquet$").matcher(name);
    if (!m.matches()) return "_meta/" + rel;
    // Bo dau 0 o thang de khuon ten giong clean (month=1, khong phai month=01).
    return "year=" + m.group(2) + "/month=" + Integer.parseInt(m.group(3)) + "/part-"
        + m.group(2) + "-" + m.group(3) + ".parquet";
  }

  private static long stream(FileSystem fs, Item it, Path dst) throws Exception {    try (InputStream in = new BufferedInputStream(new FileInputStream(it.src), 1 << 20);
         OutputStream out = fs.create(dst, (short) 1)) {
      byte[] buf = new byte[1 << 20];
      long n = 0;
      int r;
      while ((r = in.read(buf)) > 0) {
        out.write(buf, 0, r);
        n += r;
      }
      return n;
    }
  }

  private static String fmt(long bytes) {
    return String.format("%.1f", bytes / 1048576.0);
  }

  private static String mib(long bytes) {
    return String.format("%.0f", bytes / 1048576.0);
  }
}
