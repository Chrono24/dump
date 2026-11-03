package util.dump;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.junit.Test;

import util.dump.ExternalizableBean.externalize;
import util.dump.ExternalizationHelper.BytesCache;
import util.dump.ExternalizationHelper.StreamCache;

public class ExternalizationBufferRetentionTest {

   public static class LargeBean implements ExternalizableBean {

      // List fields have a dynamic length and therefore go through the thread-local buffer.
      // The large field must be written last: a following dynamic field would reset() (and thereby shrink) the buffer anyway.
      @externalize(1)
      public List<LargeBean> _nested;

      @externalize(2)
      public List<byte[]> _data;

      public LargeBean() {}

      public LargeBean( byte[] data, LargeBean nested ) {
         _data = new ArrayList<>(Collections.singletonList(data));
         _nested = nested == null ? new ArrayList<>() : new ArrayList<>(Collections.singletonList(nested));
      }
   }

   private static byte[] write( LargeBean bean ) throws Exception {
      ByteArrayOutputStream bytes = new ByteArrayOutputStream();
      try ( ObjectOutputStream out = new ObjectOutputStream(bytes) ) {
         bean.writeExternal(out);
      }
      return bytes.toByteArray();
   }

   @Test
   public void largeWriteDoesNotRetainLargeBuffer() throws Exception {
      int size = ExternalizationHelper.BYTES_CACHE_MAX_RETAINED_CAPACITY * 4;
      byte[] serialized = write(new LargeBean(new byte[size], null));
      assertThat(serialized.length).isGreaterThan(size);

      StreamCache cache = ExternalizationHelper.STREAM_CACHE.get();
      assertThat(cache._inUse).isFalse();
      assertThat(cache._bytesCache._buffer.length).isLessThanOrEqualTo(ExternalizationHelper.BYTES_CACHE_MAX_RETAINED_CAPACITY);
   }

   @Test
   public void smallWriteKeepsBufferForReuse() throws Exception {
      write(new LargeBean(new byte[10_000], null));
      StreamCache cache = ExternalizationHelper.STREAM_CACHE.get();
      byte[] retained = cache._bytesCache._buffer;
      assertThat(retained.length).isGreaterThanOrEqualTo(10_000);

      write(new LargeBean(new byte[10_000], null));
      assertThat(cache._bytesCache._buffer).isSameAs(retained);
   }

   @Test
   public void nestedLargeWriteRoundtripsAndDoesNotRetain() throws Exception {
      int size = ExternalizationHelper.BYTES_CACHE_MAX_RETAINED_CAPACITY * 2;
      LargeBean inner = new LargeBean(new byte[size], null);
      LargeBean outer = new LargeBean(new byte[size], inner);
      byte[] serialized = write(outer);

      LargeBean read = new LargeBean();
      try ( ObjectInputStream in = new ObjectInputStream(new ByteArrayInputStream(serialized)) ) {
         read.readExternal(in);
      }
      assertThat(read._data.get(0)).hasSize(size);
      assertThat(read._nested.get(0)._data.get(0)).hasSize(size);

      StreamCache cache = ExternalizationHelper.STREAM_CACHE.get();
      assertThat(cache._inUse).isFalse();
      assertThat(cache._bytesCache._buffer.length).isLessThanOrEqualTo(ExternalizationHelper.BYTES_CACHE_MAX_RETAINED_CAPACITY);
   }

   @Test
   public void failedWriteDoesNotPoisonNextWrite() throws Exception {
      LargeBean broken = new LargeBean(new byte[ExternalizationHelper.BYTES_CACHE_MAX_RETAINED_CAPACITY * 2], null);
      broken._data.add(null);
      broken._nested.add(new LargeBean(null, null) {
         @Override
         public void writeExternal( java.io.ObjectOutput out ) {
            throw new IllegalStateException("boom");
         }
      });
      try {
         write(broken);
         org.junit.Assert.fail("expected failure");
      }
      catch ( RuntimeException expected ) {
         // expected
      }

      StreamCache cache = ExternalizationHelper.STREAM_CACHE.get();
      assertThat(cache._inUse).isFalse();
      assertThat(cache._bytesCache._buffer.length).isLessThanOrEqualTo(ExternalizationHelper.BYTES_CACHE_MAX_RETAINED_CAPACITY);

      LargeBean good = new LargeBean(new byte[] { 1, 2, 3 }, null);
      LargeBean read = new LargeBean();
      try ( ObjectInputStream in = new ObjectInputStream(new ByteArrayInputStream(write(good))) ) {
         read.readExternal(in);
      }
      assertThat(read._data.get(0)).containsExactly(1, 2, 3);
   }

   @Test
   public void largeWriteOnWorkerThreadDoesNotRetainLargeBufferOnThatThread() throws Exception {
      // the buffer is retained per thread, so check the thread-local of the long-living thread that wrote
      java.util.concurrent.ExecutorService executor = java.util.concurrent.Executors.newSingleThreadExecutor();
      try {
         int size = ExternalizationHelper.BYTES_CACHE_MAX_RETAINED_CAPACITY * 4;
         executor.submit(() -> write(new LargeBean(new byte[size], null))).get();
         int retainedOnWorker = executor.submit(() -> ExternalizationHelper.STREAM_CACHE.get()._bytesCache._buffer.length).get();
         assertThat(retainedOnWorker).isLessThanOrEqualTo(ExternalizationHelper.BYTES_CACHE_MAX_RETAINED_CAPACITY);
      }
      finally {
         executor.shutdownNow();
      }
   }

   @Test
   public void bytesCacheKeepsBufferAtCapAndDropsAboveCap() {
      BytesCache atCap = new BytesCache(ExternalizationHelper.BYTES_CACHE_MAX_RETAINED_CAPACITY);
      byte[] kept = atCap._buffer;
      atCap.reset();
      assertThat(atCap._buffer).isSameAs(kept);
      assertThat(atCap.size()).isZero();

      BytesCache aboveCap = new BytesCache(ExternalizationHelper.BYTES_CACHE_MAX_RETAINED_CAPACITY + 1);
      aboveCap.reset();
      assertThat(aboveCap._buffer.length).isEqualTo(ExternalizationHelper.BYTES_CACHE_INITIAL_CAPACITY);
      assertThat(aboveCap.size()).isZero();
   }
}
