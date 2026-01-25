package util.dump.externalization;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.Test;

import util.dump.ExternalizableBean;


public class BoxedPrimitiveMigrationTest extends BaseExternalizableBeanRoundtripTest<ExternalizableBean> {

   @Test
   public void longBoxedToPrimitive() {
      givenBean(new BoxedLong(7L));
      whenBeanIsExternalizedAndRead(PrimitiveLong.class);
      assertThat(_beanThatWasRead).extracting("value").isEqualTo(7L);
   }

   @Test
   public void longBoxedToPrimitiveDefault() {
      givenBean(new BoxedLong(null));
      whenBeanIsExternalizedAndRead(PrimitiveLongWithDefault.class);
      assertThat(_beanThatWasRead).extracting("value").isEqualTo(Long.MIN_VALUE);
   }

   @Test
   public void longBoxedToPrimitiveNull() {
      givenBean(new BoxedLong(null));
      whenBeanIsExternalizedAndRead(PrimitiveLong.class);
      assertThat(_beanThatWasRead).extracting("value").isEqualTo(0L);
   }

   @Test
   public void longPrimitiveToBoxed() {
      givenBean(new PrimitiveLong(11L));
      whenBeanIsExternalizedAndRead(BoxedLong.class);
      assertThat(_beanThatWasRead).extracting("value").isEqualTo(11L);
   }

   @Test
   public void longPrimitiveToSparseBoxed() {
      givenBean(new PrimitiveLong(13L));
      whenBeanIsExternalizedAndRead(BoxedLong.class);
      assertThat(_beanThatWasRead).extracting("value").isNull();
   }

   public static final class BoxedLong implements ExternalizableBean {

      @externalize(value = 1, pLongNullValue = 13L)
      Long value;

      public BoxedLong() {}

      public BoxedLong( Long value ) {
         this.value = value;
      }
   }


   public static final class PrimitiveLong implements ExternalizableBean {

      @externalize(1)
      long value;

      public PrimitiveLong() {}

      public PrimitiveLong( long value ) {
         this.value = value;
      }
   }


   public static final class PrimitiveLongWithDefault implements ExternalizableBean {

      @externalize(value = 1, pLongNullValue = Long.MIN_VALUE)
      long value;

      public PrimitiveLongWithDefault() {}
   }

}
