package util.dump;

import com.google.common.collect.Interner;
import com.google.common.collect.Interners;


public class StringDeduplicator {

   private static final Interner<String> _stringInterner = Interners.newBuilder().weak().concurrencyLevel(Runtime.getRuntime().availableProcessors()).build();

   public static String deduplicate( String value ) {
      if ( value == null ) {
         return null;
      }

      if ( value.isEmpty() ) {
         return ""; // return interned value
      }

      return _stringInterner.intern(value);
   }

   public static String deduplicateEmpty( String value ) {
      if ( value == null ) {
         return null;
      }

      if ( value.isEmpty() ) {
         return ""; // return interned value
      }

      return value;
   }
}
