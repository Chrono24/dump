package util.dump;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import util.dump.ExternalizableBean.externalize;


/**
 * Manual stress test (not run by the build) for the thread-local buffer in {@link ExternalizableBean#writeExternal}.
 * <p>
 * Many threads serialize beans of mixed sizes (mostly small, occasionally huge, sometimes nested, sometimes failing) and verify the
 * roundtrip. After every write each thread checks that the buffer it keeps is not larger than the cap, i.e. that a large write did
 * not leave a large array behind on a long-living thread.
 * <p>
 * Run: {@code java -Xmx512m -cp target/classes:target/test-classes:<test classpath> util.dump.ExternalizationBufferStressTester [threads] [seconds]}
 * <p>
 * Exit code 0 means no corruption and no retained oversized buffer was observed.
 */
public class ExternalizationBufferStressTester {

   public static class Bean implements ExternalizableBean {

      @externalize(1)
      public List<Bean> _nested = new ArrayList<>();

      @externalize(2)
      public List<String> _strings = new ArrayList<>();

      // must be the LAST dynamic field: a following dynamic field would reset() (and thereby shrink) the buffer, hiding the leak
      @externalize(3)
      public List<byte[]> _data = new ArrayList<>();

      // fails on demand, to exercise the failure path
      public boolean _failOnWrite;

      @Override
      public void writeExternal( ObjectOutput out ) throws IOException {
         if ( _failOnWrite ) {
            throw new IllegalStateException("simulated failure");
         }
         ExternalizableBean.super.writeExternal(out);
      }

      @Override
      public boolean equals( Object o ) {
         if ( !(o instanceof Bean) ) {
            return false;
         }
         Bean b = (Bean)o;
         if ( _data.size() != b._data.size() || !_strings.equals(b._strings) || !_nested.equals(b._nested) ) {
            return false;
         }
         for ( int i = 0; i < _data.size(); i++ ) {
            if ( !Arrays.equals(_data.get(i), b._data.get(i)) ) {
               return false;
            }
         }
         return true;
      }

      @Override
      public int hashCode() {
         return _data.size() + 31 * _strings.hashCode();
      }
   }


   private static final int CAP = ExternalizationHelper.BYTES_CACHE_MAX_RETAINED_CAPACITY;

   private static final AtomicLong WRITES          = new AtomicLong();
   private static final AtomicLong HUGE_WRITES     = new AtomicLong();
   private static final AtomicLong FAILED_WRITES   = new AtomicLong();
   private static final AtomicLong CORRUPTIONS     = new AtomicLong();
   private static final AtomicLong RETAINED_VIOLATIONS = new AtomicLong();
   private static final AtomicLong MAX_RETAINED_SEEN = new AtomicLong();


   public static void main( String[] args ) throws Exception {
      int threads = args.length > 0 ? Integer.parseInt(args[0]) : 16;
      int seconds = args.length > 1 ? Integer.parseInt(args[1]) : 30;
      System.out.println("threads=" + threads + ", seconds=" + seconds + ", cap=" + CAP + " B, maxHeap=" + Runtime.getRuntime().maxMemory() / (1024 * 1024) + " MB");

      long end = System.nanoTime() + TimeUnit.SECONDS.toNanos(seconds);
      ExecutorService executor = Executors.newFixedThreadPool(threads);
      List<java.util.concurrent.Future<?>> futures = new ArrayList<>();
      for ( int t = 0; t < threads; t++ ) {
         long seed = t * 7919L + System.nanoTime();
         futures.add(executor.submit(() -> worker(new Random(seed), end)));
      }

      long nextReport = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
      while ( System.nanoTime() < end ) {
         Thread.sleep(200);
         if ( System.nanoTime() >= nextReport ) {
            nextReport += TimeUnit.SECONDS.toNanos(5);
            report();
         }
      }
      for ( java.util.concurrent.Future<?> f : futures ) {
         f.get();
      }
      executor.shutdown();
      report();

      boolean ok = CORRUPTIONS.get() == 0 && RETAINED_VIOLATIONS.get() == 0;
      System.out.println(ok ? "OK" : "FAILED");
      System.exit(ok ? 0 : 1);
   }

   private static void report() {
      Runtime rt = Runtime.getRuntime();
      System.out.printf("writes=%d (huge=%d, failed=%d) corruptions=%d retainedViolations=%d maxRetainedAfterWrite=%d B usedHeap=%d MB%n", //
         WRITES.get(), HUGE_WRITES.get(), FAILED_WRITES.get(), CORRUPTIONS.get(), RETAINED_VIOLATIONS.get(), MAX_RETAINED_SEEN.get(),
         (rt.totalMemory() - rt.freeMemory()) / (1024 * 1024));
   }

   private static void worker( Random random, long end ) {
      while ( System.nanoTime() < end ) {
         try {
            boolean fail = random.nextInt(200) == 0;
            Bean bean = newBean(random, 0);
            bean._failOnWrite = fail;

            byte[] bytes;
            try {
               bytes = write(bean);
               if ( fail ) {
                  CORRUPTIONS.incrementAndGet();
                  System.err.println("expected failure did not happen");
               }
            }
            catch ( RuntimeException e ) {
               if ( !fail ) {
                  throw e;
               }
               FAILED_WRITES.incrementAndGet();
               checkRetained();
               continue;
            }

            Bean read = new Bean();
            try ( ObjectInputStream in = new ObjectInputStream(new ByteArrayInputStream(bytes)) ) {
               read.readExternal(in);
            }
            if ( !bean.equals(read) ) {
               CORRUPTIONS.incrementAndGet();
               System.err.println("roundtrip mismatch");
            }
            WRITES.incrementAndGet();
            checkRetained();
         }
         catch ( Exception e ) {
            CORRUPTIONS.incrementAndGet();
            e.printStackTrace();
         }
      }
   }

   /** After a write has finished, the buffer kept by this thread must be small, and the cache must be free for reuse. */
   private static void checkRetained() {
      ExternalizationHelper.StreamCache cache = ExternalizationHelper.STREAM_CACHE.get();
      int retained = cache._bytesCache._buffer.length;
      MAX_RETAINED_SEEN.accumulateAndGet(retained, Math::max);
      if ( retained > CAP || cache._inUse ) {
         RETAINED_VIOLATIONS.incrementAndGet();
         System.err.println("retained buffer " + retained + " B, inUse=" + cache._inUse + " on " + Thread.currentThread().getName());
      }
   }

   private static Bean newBean( Random random, int depth ) {
      Bean b = new Bean();
      int kind = random.nextInt(1000);
      int dataBytes = kind < 3 ? CAP * (2 + random.nextInt(6)) // huge: 2 to 7 MB
         : kind < 50 ? 10_000 + random.nextInt(200_000) // medium
         : random.nextInt(200); // small
      if ( dataBytes >= CAP ) {
         HUGE_WRITES.incrementAndGet();
      }
      byte[] data = new byte[dataBytes];
      random.nextBytes(data);
      b._data.add(data);
      for ( int i = random.nextInt(3); i > 0; i-- ) {
         b._strings.add("s" + random.nextInt());
      }
      if ( depth < 3 && random.nextInt(4) == 0 ) {
         b._nested.add(newBean(random, depth + 1));
      }
      return b;
   }

   private static byte[] write( Bean bean ) throws IOException {
      ByteArrayOutputStream bytes = new ByteArrayOutputStream();
      try ( ObjectOutputStream out = new ObjectOutputStream(bytes) ) {
         bean.writeExternal(out);
      }
      return bytes.toByteArray();
   }
}
