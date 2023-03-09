package util.dump;

import util.dump.reflection.FieldFieldAccessor;


public class UniqueIndex_WithMmappedIndex_Test extends UniqueIndexTest {

   public UniqueIndex_WithMmappedIndex_Test( Integer dumpSize ) {
      super(dumpSize);
   }

   @Override
   protected <E> UniqueIndex<E> newUniqueIndex( Dump<E> dump, String fieldName ) throws NoSuchFieldException {
      return new MmappedUniqueIndex<>(dump, fieldName);
   }

   @Override
   protected <E> UniqueIndex<E> newUniqueIndex( Dump<E> dump, FieldFieldAccessor fieldAccessor ) {
      Class fieldType = fieldAccessor.getType();
      if ( fieldType == int.class || fieldType == Integer.class || fieldType == long.class || fieldType == Long.class ) {
         return new MmappedUniqueIndex<>(dump, fieldAccessor);
      } else {
         return new UniqueIndex<>(dump, fieldAccessor);
      }
   }

}
