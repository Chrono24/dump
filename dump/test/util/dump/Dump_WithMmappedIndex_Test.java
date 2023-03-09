package util.dump;

public class Dump_WithMmappedIndex_Test extends DumpTest {

   @Override
   protected <E> UniqueIndex<E> newUniqueIndex( Dump<E> dump, String fieldName ) throws NoSuchFieldException {
      return new MmappedUniqueIndex<>(dump, fieldName);
   }

}
