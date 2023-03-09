package util.dump;

public class DumpUtils_WithMmappedIndex_Test extends DumpUtilsTest {

   @Override
   protected <E> UniqueIndex<E> newUniqueIndex( Dump<E> dump, String fieldName ) throws NoSuchFieldException {
      return new MmappedUniqueIndex<>(dump, fieldName);
   }

}
