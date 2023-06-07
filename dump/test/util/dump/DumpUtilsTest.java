package util.dump;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.fail;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.util.Objects;

import org.assertj.core.util.Files;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import util.dump.DumpTest.Bean;


public class DumpUtilsTest {

   @Before
   @After
   public void deleteOldTestDumps() {
      File[] dumpFile = new File(".").listFiles(f -> f.getName().startsWith("DumpUtilsTest"));
      for ( File df : Objects.requireNonNull(dumpFile) ) {
         if ( !df.delete() ) {
            System.out.println("Failed to delete old dump file " + df);
         }
      }
   }

   @Test
   public void testDeleteDumpFilesHappy() throws Exception {
      File tmpDir = new File("target/tmp", "dumptest-" + System.currentTimeMillis());
      tmpDir.mkdirs();
      try {
         File dumpFile = File.createTempFile("dump", ".tmp", tmpDir);
         Dump<Bean> dump = new Dump<>(Bean.class, dumpFile);

         assertThat(dump._dumpFile).exists();
         assertThat(dump._metaFile).exists();

         dump.add(new Bean(1));
         dump.add(new Bean(2));
         dump.add(new Bean(3));
         dump.delete(0);
         assertThat(dump._deletionsFile).exists();

         dump.close();
         DumpUtils.deleteDumpFiles(dump);
         assertThat(dump._dumpFile).doesNotExist();
         assertThat(dump._metaFile).doesNotExist();
         assertThat(dump._deletionsFile).doesNotExist();
         assertThat(tmpDir.listFiles()).isEmpty();
      }
      finally {
         Files.delete(tmpDir);
      }
   }

   @Test
   public void testDeleteDumpFilesUnclosed() throws Exception {
      File tmpDir = new File("target/tmp", "dumptest-" + System.currentTimeMillis());
      tmpDir.mkdirs();
      try {
         File dumpFile = File.createTempFile("dump", ".tmp", tmpDir);
         Dump<Bean> dump = new Dump<>(Bean.class, dumpFile);
         try {
            DumpUtils.deleteDumpFiles(dump);
            fail("IllegalArgumentException expected");
         }
         catch ( IllegalArgumentException e ) {
            assertThat(e.getMessage()).contains("dump wasn't closed");
         }
         dump.close();
      }
      finally {
         Files.delete(tmpDir);
      }
   }

   @Test
   public void testDeleteDumpIndexFiles() throws Exception {
      File tmpDir = new File("target/tmp", "dumptest-" + System.currentTimeMillis());
      tmpDir.mkdirs();
      try {
         File dumpFile = File.createTempFile("dump", ".tmp", tmpDir);
         Dump<Bean> dump = new Dump<>(Bean.class, dumpFile);
         UniqueIndex<Bean> index = new UniqueIndex<>(dump, "_id");
         dump.add(new Bean(1));

         DumpUtils.deleteDumpIndexFiles(dump);
         assertThat(dump._indexes).isEmpty();

         assertThat(index.getLookupFile()).doesNotExist();
         assertThat(index.getMetaFile()).doesNotExist();
         assertThat(index.getUpdatesFile()).doesNotExist();

         dump.close();
         DumpUtils.deleteDumpFiles(dump);
         assertThat(tmpDir.listFiles()).isEmpty();
      }
      finally {
         Files.delete(tmpDir);
      }
   }

   @Test
   public void testDeleteDumpIndexFilesClosed() throws Exception {
      File tmpDir = new File("target/tmp", "dumptest-" + System.currentTimeMillis());
      tmpDir.mkdirs();
      try {
         File dumpFile = File.createTempFile("dump", ".tmp", tmpDir);
         Dump<Bean> dump = new Dump<>(Bean.class, dumpFile);
         dump.close();
         try {
            DumpUtils.deleteDumpIndexFiles(dump);
            fail("IllegalArgumentException expected");
         }
         catch ( IllegalArgumentException e ) {
            assertThat(e.getMessage()).contains("dump is closed");
         }
      }
      finally {
         Files.delete(tmpDir);
      }
   }

   @Test
   public void testReadWriteUtf() throws Exception {
      StringBuilder s = new StringBuilder();
      s.append("0123456789".repeat(10000));

      ByteArrayOutputStream bos = new ByteArrayOutputStream();
      DataOutputStream out = new DataOutputStream(bos);
      DumpUtils.writeUTF(s.toString(), out);
      out.close();

      DataInputStream in = new DataInputStream(new ByteArrayInputStream(bos.toByteArray()));
      String ss = DumpUtils.readUTF(in);
      in.close();

      assertThat(s.toString()).isEqualTo(ss);
   }
}
