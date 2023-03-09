package util.dump;

public class UniqueIndexDeletion_WithMmappedIndex_Test extends UniqueIndexDeletionTest {

   @Override
   protected <E> UniqueIndex<E> newUniqueIndex( Dump<E> dump, String fieldName ) throws NoSuchFieldException {
      return new MmappedUniqueIndex<>(dump, fieldName);
   }
}
