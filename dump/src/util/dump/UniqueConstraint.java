package util.dump;

public interface UniqueConstraint<E> {

   boolean contains( int key );
   boolean contains( long key );
   boolean contains( Object key );

   long[] getAllLongKeys();

   Object getKey( E o );

   int getNumKeys();

   E lookup( int key );
   E lookup( long key );
   E lookup( Object key );

   /**
    * This Exception is thrown, when trying to add a non-unique index-value to a dump.
    */
   class DuplicateKeyException extends RuntimeException {

      private static final long serialVersionUID = -7959993269514169802L;

      public DuplicateKeyException( String message ) {
         super(message);
      }
   }
}
