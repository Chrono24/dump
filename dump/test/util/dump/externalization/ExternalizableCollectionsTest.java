package util.dump.externalization;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

import org.junit.Ignore;
import org.junit.Test;

import util.dump.ExternalizableBean;
import util.dump.stream.SingleTypeObjectInputStream;
import util.dump.stream.SingleTypeObjectOutputStream;


public class ExternalizableCollectionsTest {

   protected CollectionContainer _beanToWrite;
   protected CollectionContainer _beanThatWasRead;

   @Test
   public void testArrayList() {
      testBeanIsExternalizable(new CollectionContainer(new ArrayList<>(List.of("a", "b", "c"))));
   }

   @Test
   public void testConcurrentHashMapKeySetView() {
      Map<String, String> map = new ConcurrentHashMap<>(Map.of("a", "a", "b", "b", "c", "c"));
      testBeanIsExternalizable(new CollectionContainer(map.keySet()));
   }

   @Test
   public void testConcurrentHashMapValuesView() {
      Map<String, String> map = new ConcurrentHashMap<>(Map.of("a", "a", "b", "b", "c", "c"));
      testBeanIsExternalizable(new CollectionContainer(map.values()));
   }

   @Test
   public void testConcurrentHashSet() {
      Set<String> set = ConcurrentHashMap.newKeySet();
      set.addAll(Set.of("a", "b", "c"));
      testBeanIsExternalizable(new CollectionContainer(set));
   }

   @Test
   public void testCopyOnWriteArrayList() {
      testBeanIsExternalizable(new CollectionContainer(new CopyOnWriteArrayList<>(List.of("a", "b", "c"))));
   }

   @Test
   public void testHashSet() {
      testBeanIsExternalizable(new CollectionContainer(new HashSet<>(Set.of("a", "b", "c"))));
   }

   @Test
   public void testImmutableList0() {
      testBeanIsExternalizable(new CollectionContainer(List.of()));
   }

   @Test
   public void testImmutableList1() {
      testBeanIsExternalizable(new CollectionContainer(List.of("a")));
   }

   @Test
   public void testImmutableList2() {
      testBeanIsExternalizable(new CollectionContainer(List.of("a", "b")));
   }

   @Test
   public void testImmutableListN() {
      testBeanIsExternalizable(new CollectionContainer(List.of("a", "b", "c")));
   }

   @Test
   public void testImmutableSet0() {
      testBeanIsExternalizable(new CollectionContainer(Set.of()));
   }

   @Test
   public void testImmutableSet1() {
      testBeanIsExternalizable(new CollectionContainer(Set.of("a")));
   }

   @Test
   public void testImmutableSet2() {
      testBeanIsExternalizable(new CollectionContainer(Set.of("a", "b")));
   }

   @Test
   public void testImmutableSetN() {
      testBeanIsExternalizable(new CollectionContainer(Set.of("a", "b", "c")));
   }

   @Test
   public void testLinkedHashSet() {
      // ensure order is preserved
      Set<String> set = new LinkedHashSet<>();
      set.add("c");
      set.add("b");
      set.add("a");
      testBeanIsExternalizable(new CollectionContainer(set));
   }

   @Test
   public void testTreeSet() {
      // ensure order is preserved
      Set<String> set = new TreeSet<>(Comparator.naturalOrder());
      set.addAll(Set.of("a", "b", "c"));
      testBeanIsExternalizable(new CollectionContainer(set));
   }

   @Ignore // this will not work out of the box since custom comparators are not externalized along the set
   @Test
   public void testTreeSetReversed() {
      // ensure order is preserved
      Set<String> set = new TreeSet<>(Comparator.<String>naturalOrder().reversed());
      set.addAll(Set.of("a", "b", "c"));
      testBeanIsExternalizable(new CollectionContainer(set));
   }

   @Test
   public void testTreeSetWrapped() {
      // ensure order is preserved
      Set<String> set = new TreeSet<>(Comparator.<String>naturalOrder().reversed());
      set.addAll(Set.of("a", "b", "c"));
      testBeanIsExternalizable(new CollectionContainer(Collections.unmodifiableSet(set)));
   }

   @Test
   public void testUnmodifiableCollection() {
      testBeanIsExternalizable(new CollectionContainer(Collections.unmodifiableCollection(new ArrayList<>(List.of("a", "b", "c")))));
   }

   @Test
   public void testUnmodifiableList() {
      //noinspection Java9CollectionFactory
      testBeanIsExternalizable(new CollectionContainer(Collections.unmodifiableList(new ArrayList<>(List.of("a", "b", "c")))));
   }

   @Test
   public void testUnmodifiableSet() {
      //noinspection Java9CollectionFactory
      testBeanIsExternalizable(new CollectionContainer(Collections.unmodifiableSet(new HashSet<>(Set.of("a", "b", "c")))));
   }

   protected void givenBean( CollectionContainer beanToWrite ) {
      _beanToWrite = beanToWrite;
   }

   protected void testBeanIsExternalizable( CollectionContainer bean ) {
      givenBean(bean);
      whenBeanIsExternalizedAndRead();
      thenBeansAreEqual();
   }

   protected void thenBeansAreEqual() {
      assertThat(_beanThatWasRead).usingRecursiveComparison().isEqualTo(_beanToWrite);
   }

   @SuppressWarnings("unchecked")
   protected void whenBeanIsExternalizedAndRead() {
      byte[] bytes = SingleTypeObjectOutputStream.writeSingleInstance(_beanToWrite);
      _beanThatWasRead = SingleTypeObjectInputStream.readSingleInstance((Class<CollectionContainer>)_beanToWrite.getClass(), bytes);
   }

   public static final class CollectionContainer implements ExternalizableBean {

      @externalize(1)
      private Collection<String> _collection;

      public CollectionContainer() {
      }

      public CollectionContainer( Collection<String> collection ) {
         _collection = collection;
      }

      @Override
      public boolean equals( Object o ) {
         if ( o instanceof CollectionContainer other ) {
            return this._collection.equals(other._collection);
         }
         return false;
      }

      public Collection<String> getCollection() {
         return _collection;
      }

      @Override
      public int hashCode() {
         return _collection.hashCode();
      }

      public void setCollection( Collection<String> collection ) {
         _collection = collection;
      }
   }
}
