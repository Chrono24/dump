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

}
