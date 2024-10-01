package util.dump;

import static java.time.ZoneId.of;
import static java.time.ZoneOffset.UTC;
import static util.dump.ExternalizationHelper.CLASS_CHANGED_INCOMPATIBLY;
import static util.dump.ExternalizationHelper.STREAM_CACHE;
import static util.dump.ExternalizationHelper.forName;
import static util.dump.ExternalizationHelper.getConfig;
import static util.dump.ExternalizationHelper.readBoolean;
import static util.dump.ExternalizationHelper.readByte;
import static util.dump.ExternalizationHelper.readByteArray;
import static util.dump.ExternalizationHelper.readCharacter;
import static util.dump.ExternalizationHelper.readCollection;
import static util.dump.ExternalizationHelper.readDate;
import static util.dump.ExternalizationHelper.readDateArray;
import static util.dump.ExternalizationHelper.readDouble;
import static util.dump.ExternalizationHelper.readDoubleArray;
import static util.dump.ExternalizationHelper.readExternalizableArray;
import static util.dump.ExternalizationHelper.readFloat;
import static util.dump.ExternalizationHelper.readFloatArray;
import static util.dump.ExternalizationHelper.readIntArray;
import static util.dump.ExternalizationHelper.readInteger;
import static util.dump.ExternalizationHelper.readLong;
import static util.dump.ExternalizationHelper.readLongArray;
import static util.dump.ExternalizationHelper.readMap;
import static util.dump.ExternalizationHelper.readShort;
import static util.dump.ExternalizationHelper.readString;
import static util.dump.ExternalizationHelper.readStringArray;
import static util.dump.ExternalizationHelper.readUUID;
import static util.dump.ExternalizationHelper.writeBoolean;
import static util.dump.ExternalizationHelper.writeByte;
import static util.dump.ExternalizationHelper.writeByteArray;
import static util.dump.ExternalizationHelper.writeCharacter;
import static util.dump.ExternalizationHelper.writeCollection;
import static util.dump.ExternalizationHelper.writeDate;
import static util.dump.ExternalizationHelper.writeDateArray;
import static util.dump.ExternalizationHelper.writeDouble;
import static util.dump.ExternalizationHelper.writeDoubleArray;
import static util.dump.ExternalizationHelper.writeExternalizableArray;
import static util.dump.ExternalizationHelper.writeFloat;
import static util.dump.ExternalizationHelper.writeFloatArray;
import static util.dump.ExternalizationHelper.writeIntArray;
import static util.dump.ExternalizationHelper.writeInteger;
import static util.dump.ExternalizationHelper.writeLong;
import static util.dump.ExternalizationHelper.writeLongArray;
import static util.dump.ExternalizationHelper.writeMap;
import static util.dump.ExternalizationHelper.writeShort;
import static util.dump.ExternalizationHelper.writeString;
import static util.dump.ExternalizationHelper.writeStringArray;
import static util.dump.ExternalizationHelper.writeUUID;

import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.lang.reflect.Array;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZonedDateTime;
import java.util.Arrays;
import java.util.Date;
import java.util.EnumSet;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;

import org.slf4j.LoggerFactory;

import util.dump.ExternalizationHelper.BytesCache;
import util.dump.ExternalizationHelper.ClassConfig;
import util.dump.ExternalizationHelper.FieldType;
import util.dump.ExternalizationHelper.StreamCache;
import util.dump.reflection.FieldAccessor;
import util.dump.stream.SingleTypeObjectInputStream;
import util.dump.stream.SingleTypeObjectOutputStream;


/**
 * This interface provides default implementations for <code>Externalizable</code>.<p/>
 * <p>
 * All you have to do is implement this interface with your bean (without actually providing readExternal or writeExternal)
 * and add the <code>@</code>{@link externalize} annotation for each field or getter setter pair. This annotation has a
 * parameter where you set unique, non-reusable indexes.
 * <p>
 * The (de-)serialization works even with different revisions of your bean. It is both downward and upward compatible,
 * i.e. you can add and remove fields or getter setter pairs as you like and your binary representation will
 * stay readable by both the new and the old version of your bean.<p/>
 *
 * <b>Limitations:</b>
 * <ul><li>
 * Cyclic references in the object graph are not handled (yet)! E.g. a field containing an Externalizable, which references
 * the root instance, will lead to a StackOverflowError. While this is a serious limitation, in real life it doesn't
 * happen too often. In most cases, you can work around this issue, by not externalizing such fields multiple times
 * and wiring them by hand, after externalization. Overwrite <code>readExternal()</code> to do so.
 * </li><li>
 * Downward and upward compatibility means, that while the externalization does not fail, unknown fields are ignored,
 * and unknown Enum values are set to null or left out in EnumSets.
 * </li><li>
 * Downward and upward compatibility will not work, if you reuse indexes between different revisions of your bean,
 * i.e. you may never change the field type or any of the externalize.default*Types of a field annotated with a given index.
 * </li><li>
 * While externalization with this method is about 3-6 times faster than serialization (depending on the amount
 * of non-primitive or array members), hand written externalization is still about 40% faster, because no reflection
 * is used and upwards/downwards compatibility is not taken care of. This method of serialization is a bit faster
 * than Google's protobuffers in most cases. For optimal performance use jre 1.6+ and the <code>-server</code> switch.
 * </li><li>
 * All types are allowed for your members, but if your member is not included in the following list of supported
 * types, serialization falls back to normal java.io.Serializable mechanisms by using {@link ObjectOutput#writeObject(Object)},
 * which is slow and breaks downward and upward compatibility.
 * These are the supported types (see also {@link FieldType}):
 * <ul><li>
 * primitive fields (<code>int</code>, <code>float</code>, ...) and single-dimensional arrays containing primitives
 * </li><li>
 * all <code>Number</code> classes (<code>Integer</code>, <code>Float</code>, ...)
 * </li><li>
 * <code>String</code> and <code>String[]</code>
 * </li><li>
 * <code>Date</code> and <code>Date[]</code>
 * </li><li>
 * single and two-dimensional arrays of any <code>Externalizable</code>
 * </li><li>
 * generic Lists or Sets of any <code>Externalizable</code> type, i.e. <code>List&lt;Externalizable&gt;</code> or
 * <code>Set&lt;Externalizable&gt;</code>
 * </li><li>
 * generic Lists or Sets of <code>String</code> type, i.e. <code>List&lt;String&gt;</code> or <code>Set&lt;String&gt;</code>
 * </li><li>
 * generic Lists or Sets of some <code>Number</code> types, i.e. <code>List&lt;Integer&gt;</code> or <code>Set&lt;Long&gt;</code>
 * </li><li>
 * generic Maps of <code>String</code>, some <code>Number</code> types, or any <code>Externalizable</code> type, i.e.
 * <code>Map&lt;String, Externalizable&gt;</code> or <code>Map&lt;Externalizable, Long&gt;</code>
 * </li>
 * </ul>
 * Currently unsupported (i.e. slow and not compatible with {@link util.dump.stream.SingleTypeObjectStreamProvider})
 * are multi-dimensional primitive arrays, any array of <code>Numbers</code>, multi-dim <code>String</code> or
 * <code>Date</code> arrays.
 * </li><li>
 * Any type to be externalized must have a public nullary constructor. This applies to all fields and their dependant instances,
 * i.e. for all <code>Collections</code> and all <code>Externalizables</code>. Beware that instances like the ones created with
 * <code>Collections.synchronizedSet(.)</code> do not have a public constructor.
 * </li><li>
 * For all <code>Collections</code> only the type and the included data is externalized. Something like a custom comparator in
 * a <code>TreeSet</code> gets lost.
 * </li><li>
 * While annotated fields can be any of public, protected, package protected or private, annotated methods must be public.
 * </li><li>
 * Unless the system property <code>ExternalizableBean.USE_UNSAFE_FIELD_ACCESSORS</code> is set to <code>false</code>
 * a daring hack is used for making field access using reflection faster. That's why you should annotate fields rather than
 * methods, unless you need some transformation before or after serialization.
 * </li>
 * </ul>
 *
 * @see externalize
 */
@SuppressWarnings({ "unchecked", "unused" })
public interface ExternalizableBean extends Externalizable {

   long serialVersionUID = -1816997029156670474L;

   static void skipFully( ObjectInput in, int len ) throws IOException {
      if ( len > 0 ) {
         final byte[] buf = new byte[Math.min(len, 4000)]; // limit to slightly below page size
         int remaining = len, skip;
         do {
            skip = Math.min(remaining, buf.length);
            in.readFully(buf, 0, skip);
            remaining -= skip;
         }
         while ( remaining > 0 );
      }
   }

   /**
    * Clones this instance by externalizing it to bytes and reading these bytes again.
    * This leads to a deep copy, but only for all fields annotated by @externalize(.).
    */
   default <T extends Externalizable> T cloneDeeply() {
      byte[] bytes = SingleTypeObjectOutputStream.writeSingleInstance(this);
      ExternalizableBean clone = SingleTypeObjectInputStream.readSingleInstance(getClass(), bytes);
      return (T)clone;
   }

   /**
    * Compares this instance to another by externalizing both to bytes and comparing the bytes.
    * This means it does a deep equals operation, but ignores all fields/methods that are not
    * externalized.
    */
   default <T extends Externalizable> boolean deepEquals( T other ) {
      if ( other == null ) {
         return false;
      }
      byte[] bytes = SingleTypeObjectOutputStream.writeSingleInstance(this);
      byte[] otherBytes = SingleTypeObjectOutputStream.writeSingleInstance(other);
      return Arrays.equals(bytes, otherBytes);
   }

   @Override
   default void readExternal( ObjectInput in ) throws IOException, ClassNotFoundException {
      try {
         ClassConfig config = getConfig(getClass());

         int fieldNumberToRead = readExternalFieldNumber(in);

         FieldAccessor[] fieldAccessors = config._fieldAccessors;
         byte[] fieldIndexes = config._fieldIndexes;
         FieldType[] fieldTypes = config._fieldTypes;
         Class[] defaultTypes = config._defaultTypes;
         int j = 0;
         for ( int i = 0; i < fieldNumberToRead; i++ ) {
            int fieldIndex = in.readByte() & 0xff;
            byte fieldTypeId = in.readByte();

            /* We expect fields to be stored in ascending fieldIndex order.
             * That's why we can find the appropriate fieldIndex in our sorted fieldIndexes array by skipping. */
            while ( (fieldIndexes[j] & 0xff) < fieldIndex && j < fieldIndexes.length - 1 ) {
               j++;
            }

            FieldType inputFt, configuredFt;
            FieldAccessor f = null;
            Class defaultType = null;
            if ( (fieldIndexes[j] & 0xff) == fieldIndex ) {
               final FieldType fft = configuredFt = inputFt = fieldTypes[j];
               f = fieldAccessors[j];
               defaultType = defaultTypes[j];
               if ( fieldTypeId != inputFt._id ) {
                  if ( fieldTypeId == FieldType.EnumOld._id && inputFt._id == FieldType.Enum._id ) {
                     inputFt = FieldType.EnumOld;
                  } else if ( fieldTypeId == FieldType.EnumSetOld._id && inputFt._id == FieldType.EnumSet._id ) {
                     inputFt = FieldType.EnumSetOld;
                  } else if ( fieldTypeId == FieldType.SetOfStrings._id && inputFt._id == FieldType.Set._id ) {
                     inputFt = FieldType.SetOfStrings;
                  } else if ( fieldTypeId == FieldType.ListOfStrings._id && inputFt._id == FieldType.List._id ) {
                     inputFt = FieldType.ListOfStrings;
                  } else if ( fieldTypeId == FieldType.Set._id && inputFt._id == FieldType.SetOfStrings._id ) {
                     inputFt = FieldType.Set;
                  } else if ( fieldTypeId == FieldType.List._id && inputFt._id == FieldType.ListOfStrings._id ) {
                     inputFt = FieldType.List;
                  } else if ( isCompatible(FieldType.forId(fieldTypeId), fft) ) {
                     inputFt = FieldType.forId(fieldTypeId);
                  } else if ( Boolean.TRUE.equals(CLASS_CHANGED_INCOMPATIBLY.computeIfAbsent(getClass(), clazz -> {
                     LoggerFactory.getLogger(clazz).error("The field type of index " + fieldIndex + //
                           " in " + clazz.getSimpleName() + //
                           " appears to have changed from " + FieldType.forId(fieldTypeId) + //
                           " (version in dump) to " + fft + " (current class version)." + //
                           " This change breaks downward compatibility, see JavaDoc for details." + //
                           " This warning will appear only once.");
                     return Boolean.TRUE;
                  })) ) {
                     // read it without exception, but ignore the data
                     inputFt = FieldType.forId(fieldTypeId);
                     f = null;
                  }
               }
            } else { // unknown field
               configuredFt = inputFt = FieldType.forId(fieldTypeId);
            }

            Objects.requireNonNull(inputFt, "Invalid field type " + (fieldTypeId & 0xff) + " for field index " + fieldIndex);

            if ( inputFt.isLengthDynamic() ) {
               int len = in.readInt();
               if ( f == null ) { // unknown field, skip it
                  skipFully(in, len);
                  continue;
               }
            }

            switch ( inputFt ) {
            case pInt: {
               int d = in.readInt();
               if ( f != null ) {
                  switch ( configuredFt ) {
                  case pInt -> f.setInt(this, d);
                  case Integer -> f.set(this, d);
                  default -> throw new IllegalStateException();
                  }
               }
               break;
            }
            case pBoolean: {
               boolean d = in.readBoolean();
               if ( f != null ) {
                  switch ( configuredFt ) {
                  case pBoolean -> f.setBoolean(this, d);
                  case Boolean -> f.set(this, d);
                  default -> throw new IllegalStateException();
                  }
               }
               break;
            }
            case pByte: {
               byte d = in.readByte();
               if ( f != null ) {
                  switch ( configuredFt ) {
                  case pByte -> f.setByte(this, d);
                  case Byte -> f.set(this, d);
                  default -> throw new IllegalStateException();
                  }
               }
               break;
            }
            case pChar: {
               char d = in.readChar();
               if ( f != null ) {
                  switch ( configuredFt ) {
                  case pChar -> f.setChar(this, d);
                  case Character -> f.set(this, d);
                  default -> throw new IllegalStateException();
                  }
               }
               break;
            }
            case pDouble: {
               double d = in.readDouble();
               if ( f != null ) {
                  switch ( configuredFt ) {
                  case pDouble -> f.setDouble(this, d);
                  case Double -> f.set(this, d);
                  default -> throw new IllegalStateException();
                  }
               }
               break;
            }
            case pFloat: {
               float d = in.readFloat();
               if ( f != null ) {
                  switch ( configuredFt ) {
                  case pFloat -> f.setFloat(this, d);
                  case Float -> f.set(this, d);
                  default -> throw new IllegalStateException();
                  }
               }
               break;
            }
            case pLong: {
               long d = in.readLong();
               if ( f != null ) {
                  switch ( configuredFt ) {
                  case pLong -> f.setLong(this, d);
                  case Long -> f.set(this, d);
                  default -> throw new IllegalStateException();
                  }
               }
               break;
            }
            case pShort: {
               short d = in.readShort();
               if ( f != null ) {
                  switch ( configuredFt ) {
                  case pShort -> f.setShort(this, d);
                  case Short -> f.set(this, d);
                  default -> throw new IllegalStateException();
                  }
               }
               break;
            }
            case String: {
               String s = readString(in);
               if ( f != null ) {
                  f.set(this, s);
               }
               break;
            }
            case Date: {
               Date d = readDate(in);
               if ( f != null ) {
                  f.set(this, d);
               }
               break;
            }
            case UUID: {
               UUID uuid = readUUID(in);
               if ( f != null ) {
                  f.set(this, uuid);
               }
               break;
            }
            case Externalizable: {
               Externalizable instance = null;
               boolean isNotNull = in.readBoolean();
               if ( isNotNull ) {
                  Class<? extends Externalizable> c = in.readBoolean() ? defaultType : forName(in.readUTF(), config);
                  instance = c.newInstance();
                  instance.readExternal(in);
               }
               f.set(this, instance);
               break;
            }
            case Integer: {
               Integer d = readInteger(in);
               if ( f != null ) {
                  switch ( configuredFt ) {
                  case pInt -> f.setInt(this, d == null ? config._annotations[j].pIntNullValue() : d);
                  case Integer -> f.set(this, d);
                  default -> throw new IllegalStateException();
                  }
               }
               break;
            }
            case Boolean: {
               Boolean d = readBoolean(in);
               if ( f != null ) {
                  switch ( configuredFt ) {
                  case pBoolean -> //noinspection SimplifiableConditionalExpression
                        f.setBoolean(this, d == null ? config._annotations[j].pBooleanNullValue() : false);
                  case Boolean -> f.set(this, d);
                  default -> throw new IllegalStateException();
                  }
               }
               break;
            }
            case Byte: {
               Byte d = readByte(in);
               if ( f != null ) {
                  switch ( configuredFt ) {
                  case pByte -> f.setByte(this, d == null ? config._annotations[j].pByteNullValue() : d);
                  case Byte -> f.set(this, d);
                  default -> throw new IllegalStateException();
                  }
               }
               break;
            }
            case Character: {
               Character d = readCharacter(in);
               if ( f != null ) {
                  switch ( configuredFt ) {
                  case pChar -> f.setChar(this, d == null ? config._annotations[j].pCharNullValue() : d);
                  case Character -> f.set(this, d);
                  default -> throw new IllegalStateException();
                  }
               }
               break;
            }
            case Double: {
               Double d = readDouble(in);
               if ( f != null ) {
                  switch ( configuredFt ) {
                  case pDouble -> f.setDouble(this, d == null ? config._annotations[j].pDoubleNullValue() : d);
                  case Double -> f.set(this, d);
                  default -> throw new IllegalStateException();
                  }
               }
               break;
            }
            case Float: {
               Float d = readFloat(in);
               if ( f != null ) {
                  switch ( configuredFt ) {
                  case pFloat -> f.setFloat(this, d == null ? config._annotations[j].pFloatNullValue() : d);
                  case Float -> f.set(this, d);
                  default -> throw new IllegalStateException();
                  }
               }
               break;
            }
            case Long: {
               Long d = readLong(in);
               if ( f != null ) {
                  switch ( configuredFt ) {
                  case pLong -> f.setLong(this, d == null ? config._annotations[j].pLongNullValue() : d);
                  case Long -> f.set(this, d);
                  default -> throw new IllegalStateException();
                  }
               }
               break;
            }
            case Short: {
               Short d = readShort(in);
               if ( f != null ) {
                  switch ( configuredFt ) {
                  case pShort -> f.setShort(this, d == null ? config._annotations[j].pShortNullValue() : d);
                  case Short -> f.set(this, d);
                  default -> throw new IllegalStateException();
                  }
               }
               break;
            }
            case BigDecimal: {
               BigDecimal d = null;
               boolean isNotNull = in.readBoolean();
               if ( isNotNull ) {
                  byte[] bytes = readByteArray(in);
                  int scale = in.readInt();
                  d = new BigDecimal(new BigInteger(bytes), scale);
               }
               if ( f != null ) {
                  f.set(this, d);
               }
               break;
            }
            case LocalTime: {
               LocalTime t = null;
               boolean isNotNull = in.readBoolean();
               if ( isNotNull ) {
                  int hour = in.readByte();
                  int minute = in.readByte();
                  int second = in.readByte();
                  int nano = in.readInt();
                  t = LocalTime.of(hour, minute, second, nano);
               }

               if ( f != null ) {
                  f.set(this, t);
               }
               break;
            }
            case LocalDateTime: {
               LocalDateTime d = null;
               boolean isNotNull = in.readBoolean();
               if ( isNotNull ) {
                  long t = in.readLong();
                  int ns = in.readInt();
                  d = LocalDateTime.ofEpochSecond(t, ns, UTC);
               }
               if ( f != null ) {
                  f.set(this, d);
               }
               break;
            }
            case LocalDate: {
               LocalDate d = null;
               boolean isNotNull = in.readBoolean();
               if ( isNotNull ) {
                  int year = in.readInt();
                  int month = in.readByte();
                  int dayOfMonth = in.readByte();
                  d = LocalDate.of(year, month, dayOfMonth);
               }
               if ( f != null ) {
                  f.set(this, d);
               }
               break;
            }
            case Instant: {
               Instant t = null;
               boolean isNotNull = in.readBoolean();
               if ( isNotNull ) {
                  long seconds = in.readLong();
                  int nanos = in.readInt();
                  t = Instant.ofEpochSecond(seconds, nanos);
               }
               if ( f != null ) {
                  f.set(this, t);
               }
               break;
            }
            case ZonedDateTime: {
               ZonedDateTime d = null;
               boolean isNotNull = in.readBoolean();
               if ( isNotNull ) {
                  long t = in.readLong();
                  String zoneId = in.readUTF();
                  d = ZonedDateTime.ofInstant(Instant.ofEpochMilli(t), of(zoneId));
               }
               if ( f != null ) {
                  f.set(this, d);
               }
               break;
            }
            case pByteArray: {
               byte[] d = readByteArray(in);
               if ( f != null ) {
                  f.set(this, d);
               }
               break;
            }
            case pByteArrayArray: {
               byte[][] d = null;
               boolean isNotNull = in.readBoolean();
               if ( isNotNull ) {
                  int size = in.readInt();
                  d = new byte[size][];
                  for ( int k = 0; k < size; k++ ) {
                     d[k] = readByteArray(in);
                  }
               }
               if ( f != null ) {
                  f.set(this, d);
               }
               break;
            }
            case pDoubleArray: {
               double[] d = readDoubleArray(in);
               if ( f != null ) {
                  f.set(this, d);
               }
               break;
            }
            case pDoubleArrayArray: {
               double[][] d = null;
               boolean isNotNull = in.readBoolean();
               if ( isNotNull ) {
                  int size = in.readInt();
                  d = new double[size][];
                  for ( int k = 0; k < size; k++ ) {
                     d[k] = readDoubleArray(in);
                  }
               }
               if ( f != null ) {
                  f.set(this, d);
               }
               break;
            }
            case pFloatArray: {
               float[] d = readFloatArray(in);
               if ( f != null ) {
                  f.set(this, d);
               }
               break;
            }
            case pFloatArrayArray: {
               float[][] d = null;
               boolean isNotNull = in.readBoolean();
               if ( isNotNull ) {
                  int size = in.readInt();
                  d = new float[size][];
                  for ( int k = 0; k < size; k++ ) {
                     d[k] = readFloatArray(in);
                  }
               }
               if ( f != null ) {
                  f.set(this, d);
               }
               break;
            }
            case pIntArray: {
               int[] d = readIntArray(in);
               if ( f != null ) {
                  f.set(this, d);
               }
               break;
            }
            case pIntArrayArray: {
               int[][] d = null;
               boolean isNotNull = in.readBoolean();
               if ( isNotNull ) {
                  int size = in.readInt();
                  d = new int[size][];
                  for ( int k = 0; k < size; k++ ) {
                     d[k] = readIntArray(in);
                  }
               }
               if ( f != null ) {
                  f.set(this, d);
               }
               break;
            }
            case pLongArray: {
               long[] d = readLongArray(in);
               if ( f != null ) {
                  f.set(this, d);
               }
               break;
            }
            case pLongArrayArray: {
               long[][] d = null;
               boolean isNotNull = in.readBoolean();
               if ( isNotNull ) {
                  int size = in.readInt();
                  d = new long[size][];
                  for ( int k = 0; k < size; k++ ) {
                     d[k] = readLongArray(in);
                  }
               }
               if ( f != null ) {
                  f.set(this, d);
               }
               break;
            }
            case StringArray: {
               String[] d = readStringArray(in);
               if ( f != null ) {
                  f.set(this, d);
               }
               break;
            }
            case StringArrayArray: {
               String[][] d = null;
               boolean isNotNull = in.readBoolean();
               if ( isNotNull ) {
                  int size = in.readInt();
                  d = new String[size][];
                  for ( int k = 0; k < size; k++ ) {
                     d[k] = readStringArray(in);
                  }
               }
               if ( f != null ) {
                  f.set(this, d);
               }
               break;
            }
            case DateArray: {
               Date[] d = readDateArray(in);
               if ( f != null ) {
                  f.set(this, d);
               }
               break;
            }
            case DateArrayArray: {
               Date[][] d = null;
               boolean isNotNull = in.readBoolean();
               if ( isNotNull ) {
                  int size = in.readInt();
                  d = new Date[size][];
                  for ( int k = 0; k < size; k++ ) {
                     d[k] = readDateArray(in);
                  }
               }
               if ( f != null ) {
                  f.set(this, d);
               }
               break;
            }
            case Collection:
            case List:
            case Set: {
               readCollection(in, f, defaultType, config._defaultGenericTypes0[j], this, config);
               break;
            }
            case ListOfStrings:
            case SetOfStrings: {
               readCollection(in, f, defaultType, String.class, this, config);
               break;
            }
            case ExternalizableArray: {
               Externalizable[] d = readExternalizableArray(in, f.getType().getComponentType(), config);
               f.set(this, d);
               break;
            }
            case ExternalizableArrayArray: {
               Externalizable[][] d = null;
               boolean isNotNull = in.readBoolean();
               if ( isNotNull ) {
                  int size = in.readInt();
                  Class externalizableClass = f.getType().getComponentType();
                  d = (Externalizable[][])Array.newInstance(externalizableClass, size);
                  for ( int k = 0, length = d.length; k < length; k++ ) {
                     d[k] = readExternalizableArray(in, f.getType().getComponentType().getComponentType(), config);
                  }
               }
               f.set(this, d);
               break;
            }
            case EnumOld: {
               Enum e = null;
               boolean isNotNull = in.readBoolean();
               if ( isNotNull ) {
                  if ( f != null ) {
                     Class<? extends Enum> c = f.getType();
                     Enum[] values = c.getEnumConstants();
                     int b = in.readInt();
                     if ( b < values.length ) {
                        e = values[b];
                     }
                  } else {
                     in.readInt();
                  }
               }
               if ( f != null ) {
                  f.set(this, e);
               }
               break;
            }
            case EnumSetOld: {
               EnumSet enumSet = null;
               boolean isNotNull = in.readBoolean();
               if ( isNotNull ) {
                  if ( f != null ) {
                     Class<? extends Enum> c = f.getGenericTypes()[0];
                     enumSet = EnumSet.noneOf(c);
                     Enum[] values = c.getEnumConstants();
                     long l = in.readLong();
                     for ( int k = 0, length = values.length; k < length; k++ ) {
                        if ( (l & (1L << k)) != 0 ) {
                           enumSet.add(values[k]);
                        }
                     }
                  } else {
                     in.readLong();
                  }
               }
               if ( f != null ) {
                  f.set(this, enumSet);
               }
               break;
            }
            case Enum: {
               Enum e = null;
               boolean isNotNull = in.readBoolean();
               if ( isNotNull ) {
                  String enumName = DumpUtils.readUTF(in);
                  if ( f != null ) {
                     try {
                        Class<? extends Enum> c = f.getType();
                        e = Enum.valueOf(c, enumName);
                     }
                     catch ( IllegalArgumentException unknownEnumConstantException ) {
                        // an enum constant was added or removed and our class is not compatible - as always in this class, we silently ignore the unknown value
                     }
                  }
               }
               if ( f != null ) {
                  f.set(this, e);
               }
               break;
            }
            case EnumSet: {
               EnumSet enumSet = null;
               boolean isNotNull = in.readBoolean();
               if ( isNotNull ) {
                  int size = in.readInt();
                  if ( f != null ) {
                     Class<? extends Enum> c = f.getGenericTypes()[0];
                     enumSet = EnumSet.noneOf(c);
                     for ( int k = 0; k < size; k++ ) {
                        try {
                           enumSet.add(Enum.valueOf(c, DumpUtils.readUTF(in)));
                        }
                        catch ( IllegalArgumentException unknownEnumConstantException ) {
                           // an enum constant was added or removed and our class is not compatible - as always in this class, we silently ignore the unknown value
                        }
                     }
                  } else {
                     for ( int k = 0; k < size; k++ ) {
                        DumpUtils.readUTF(in);
                     }
                  }
               }
               if ( f != null ) {
                  f.set(this, enumSet);
               }
               break;
            }
            case Map: {
               readMap(in, f, defaultType, config._defaultGenericTypes0[j], config._defaultGenericTypes1[j], this, config);
               break;
            }
            case Padding: {
               int len = in.readShort();
               skipFully(in, len);
               break;
            }
            default: {
               Object o = null;
               boolean isNotNull = in.readBoolean();
               if ( isNotNull ) {
                  o = in.readObject();
               }
               if ( f != null ) {
                  f.set(this, o);
               }
               //               throw new IllegalArgumentException("The field type " + fieldTypes[i] + " in class " + getClass()
               //                  + " is unsupported by util.dump.ExternalizableBean.");
            }
            }
         }
      }
      catch ( EOFException e ) {
         throw e;
      }
      catch ( Throwable e ) {
         throw new RuntimeException("Failed to read externalized instance. Maybe the field order was changed? class " + getClass(), e);
      }
   }

   /**
    * Method extracted, so that subclasses can adjust parsing (currently used for migrations only)
    */
   default int readExternalFieldNumber( ObjectInput in ) throws IOException {
      return in.readByte() & 0xff;
   }

   @Override
   default void writeExternal( ObjectOutput out ) throws IOException {
      try {
         ClassConfig _config = getConfig(getClass());

         int bytesWrittenToStream = 0;
         DataOutputStream dataOutputStream = null;
         if ( out instanceof DataOutputStream ) {
            dataOutputStream = (DataOutputStream)out;
            bytesWrittenToStream = dataOutputStream.size();
         }

         ObjectOutput originalOut = out;
         StreamCache streamCache = null;
         BytesCache bytesCache = null;
         ObjectOutput cachingOut = null;

         FieldAccessor[] fieldAccessors = _config._fieldAccessors;
         byte[] fieldIndexes = _config._fieldIndexes;
         FieldType[] fieldTypes = _config._fieldTypes;
         Class[] defaultTypes = _config._defaultTypes;
         out.writeByte(fieldAccessors.length);
         for ( int i = 0, length = fieldAccessors.length; i < length; i++ ) {
            FieldAccessor f = fieldAccessors[i];
            FieldType ft = fieldTypes[i];
            Class defaultType = defaultTypes[i];

            out.writeByte(fieldIndexes[i]);
            out.writeByte(ft._id);

            if ( ft.isLengthDynamic() ) {
               if ( streamCache == null ) {
                  streamCache = STREAM_CACHE.get();
                  if ( streamCache._inUse ) {
                     // if our instance contains another instance of the same type, we cannot re-use the stream cache, so we create a fresh one
                     streamCache = new StreamCache();
                  }
                  bytesCache = streamCache._bytesCache;
                  cachingOut = streamCache._objectOutput;
               }
               streamCache._inUse = true;
               bytesCache.reset();
               out = cachingOut;
            }

            switch ( ft ) {
            case pInt: {
               out.writeInt(f.getInt(this));
               break;
            }
            case pBoolean: {
               out.writeBoolean(f.getBoolean(this));
               break;
            }
            case pByte: {
               out.writeByte(f.getByte(this));
               break;
            }
            case pChar: {
               out.writeChar(f.getChar(this));
               break;
            }
            case pDouble: {
               out.writeDouble(f.getDouble(this));
               break;
            }
            case pFloat: {
               out.writeFloat(f.getFloat(this));
               break;
            }
            case pLong: {
               out.writeLong(f.getLong(this));
               break;
            }
            case pShort: {
               out.writeShort(f.getShort(this));
               break;
            }
            case String: {
               String s = (String)f.get(this);
               writeString(out, s);
               break;
            }
            case Date: {
               Date s = (Date)f.get(this);
               writeDate(out, s);
               break;
            }
            case UUID: {
               UUID u = (UUID)f.get(this);
               writeUUID(out, u);
               break;
            }
            case Externalizable: {
               Externalizable instance = (Externalizable)f.get(this);
               out.writeBoolean(instance != null);
               if ( instance != null ) {
                  Class c = instance.getClass();
                  boolean isDefault = c.equals(defaultType);
                  out.writeBoolean(isDefault);
                  if ( !isDefault ) {
                     out.writeUTF(c.getName());
                  }
                  instance.writeExternal(out);
               }
               break;
            }
            case Integer: {
               Integer s = (Integer)f.get(this);
               writeInteger(out, s);
               break;
            }
            case Boolean: {
               Boolean s = (Boolean)f.get(this);
               writeBoolean(out, s);
               break;
            }
            case Byte: {
               Byte s = (Byte)f.get(this);
               writeByte(out, s);
               break;
            }
            case Character: {
               Character s = (Character)f.get(this);
               writeCharacter(out, s);
               break;
            }
            case Double: {
               Double s = (Double)f.get(this);
               writeDouble(out, s);
               break;
            }
            case Float: {
               Float s = (Float)f.get(this);
               writeFloat(out, s);
               break;
            }
            case Long: {
               Long s = (Long)f.get(this);
               writeLong(out, s);
               break;
            }
            case Short: {
               Short s = (Short)f.get(this);
               writeShort(out, s);
               break;
            }
            case BigDecimal: {
               BigDecimal d = (BigDecimal)f.get(this);
               out.writeBoolean(d != null);
               if ( d != null ) {
                  byte[] bytes = d.unscaledValue().toByteArray();
                  writeByteArray(bytes, out);
                  out.writeInt(d.scale());
               }
               break;
            }
            case LocalTime: {
               LocalTime t = (LocalTime)f.get(this);
               out.writeBoolean(t != null);
               if ( t != null ) {
                  out.writeByte(t.getHour());
                  out.writeByte(t.getMinute());
                  out.writeByte(t.getSecond());
                  out.writeInt(t.getNano());
               }
               break;
            }
            case LocalDateTime: {
               LocalDateTime d = (LocalDateTime)f.get(this);
               out.writeBoolean(d != null);
               if ( d != null ) {
                  out.writeLong(d.toEpochSecond(UTC));
                  out.writeInt(d.getNano());
               }
               break;
            }
            case LocalDate: {
               LocalDate d = (LocalDate)f.get(this);
               out.writeBoolean(d != null);
               if ( d != null ) {
                  out.writeInt(d.getYear());
                  out.writeByte(d.getMonthValue());
                  out.writeByte(d.getDayOfMonth());
               }
               break;
            }
            case Instant: {
               Instant t = (Instant)f.get(this);
               out.writeBoolean(t != null);
               if ( t != null ) {
                  out.writeLong(t.getEpochSecond());
                  out.writeInt(t.getNano());
               }
               break;
            }
            case ZonedDateTime: {
               ZonedDateTime d = (ZonedDateTime)f.get(this);
               out.writeBoolean(d != null);
               if ( d != null ) {
                  out.writeLong(d.toInstant().toEpochMilli());
                  out.writeUTF(d.getZone().getId());
               }
               break;
            }
            case pByteArray: {
               byte[] d = (byte[])f.get(this);
               writeByteArray(d, out);
               break;
            }
            case pByteArrayArray: {
               byte[][] d = (byte[][])f.get(this);
               out.writeBoolean(d != null);
               if ( d != null ) {
                  out.writeInt(d.length);
                  for ( byte[] dd : d ) {
                     writeByteArray(dd, out);
                  }
               }
               break;
            }
            case pDoubleArray: {
               double[] d = (double[])f.get(this);
               writeDoubleArray(d, out);
               break;
            }
            case pDoubleArrayArray: {
               double[][] d = (double[][])f.get(this);
               out.writeBoolean(d != null);
               if ( d != null ) {
                  out.writeInt(d.length);
                  for ( double[] dd : d ) {
                     writeDoubleArray(dd, out);
                  }
               }
               break;
            }
            case pFloatArray: {
               float[] d = (float[])f.get(this);
               writeFloatArray(d, out);
               break;
            }
            case pFloatArrayArray: {
               float[][] d = (float[][])f.get(this);
               out.writeBoolean(d != null);
               if ( d != null ) {
                  out.writeInt(d.length);
                  for ( float[] dd : d ) {
                     writeFloatArray(dd, out);
                  }
               }
               break;
            }
            case pIntArray: {
               int[] d = (int[])f.get(this);
               writeIntArray(d, out);
               break;
            }
            case pIntArrayArray: {
               int[][] d = (int[][])f.get(this);
               out.writeBoolean(d != null);
               if ( d != null ) {
                  out.writeInt(d.length);
                  for ( int[] dd : d ) {
                     writeIntArray(dd, out);
                  }
               }
               break;
            }
            case pLongArray: {
               long[] d = (long[])f.get(this);
               writeLongArray(d, out);
               break;
            }
            case pLongArrayArray: {
               long[][] d = (long[][])f.get(this);
               out.writeBoolean(d != null);
               if ( d != null ) {
                  out.writeInt(d.length);
                  for ( long[] dd : d ) {
                     writeLongArray(dd, out);
                  }
               }
               break;
            }
            case StringArray: {
               String[] d = (String[])f.get(this);
               writeStringArray(d, out);
               break;
            }
            case StringArrayArray: {
               String[][] d = (String[][])f.get(this);
               out.writeBoolean(d != null);
               if ( d != null ) {
                  out.writeInt(d.length);
                  for ( String[] dd : d ) {
                     writeStringArray(dd, out);
                  }
               }
               break;
            }
            case DateArray: {
               Date[] d = (Date[])f.get(this);
               writeDateArray(d, out);
               break;
            }
            case DateArrayArray: {
               Date[][] d = (Date[][])f.get(this);
               out.writeBoolean(d != null);
               if ( d != null ) {
                  out.writeInt(d.length);
                  for ( Date[] dd : d ) {
                     writeDateArray(dd, out);
                  }
               }
               break;
            }
            case Collection:
            case List:
            case Set: {
               writeCollection(out, f, defaultType, _config._defaultGenericTypes0[i], this);
               break;
            }
            case ListOfStrings:
            case SetOfStrings: {
               writeCollection(out, f, defaultType, String.class, this);
               break;
            }
            case ExternalizableArray: {
               Externalizable[] d = (Externalizable[])f.get(this);
               writeExternalizableArray(out, d, defaultType);
               break;
            }
            case ExternalizableArrayArray: {
               Externalizable[][] d = (Externalizable[][])f.get(this);
               out.writeBoolean(d != null);
               if ( d != null ) {
                  out.writeInt(d.length);
                  for ( int j = 0, llength = d.length; j < llength; j++ ) {
                     writeExternalizableArray(out, d[j], defaultType);
                  }
               }
               break;
            }
            case Enum: {
               Enum e = (Enum)f.get(this);
               out.writeBoolean(e != null);
               if ( e != null ) {
                  DumpUtils.writeUTF(e.name(), out);
               }
               break;
            }
            case EnumSet: {
               EnumSet enumSet = (EnumSet)f.get(this);
               out.writeBoolean(enumSet != null);
               if ( enumSet != null ) {
                  out.writeInt(enumSet.size());
                  for ( Enum e : (Set<Enum>)enumSet ) {
                     DumpUtils.writeUTF(e.name(), out); // not writeString(), since the value cannot be null
                  }
               }
               break;
            }
            case Map: {
               writeMap(out, f, defaultType, _config._defaultGenericTypes0[i], _config._defaultGenericTypes1[i], this);
               break;
            }
            case Padding: {
               short padding = 0;
               if ( dataOutputStream != null && _config._sizeModulo > 0 ) {
                  bytesWrittenToStream = dataOutputStream.size() - bytesWrittenToStream;
                  bytesWrittenToStream += 2; // we need a short to store the amount of padding
                  int modulo = bytesWrittenToStream % _config._sizeModulo;
                  padding = (short)(_config._sizeModulo - modulo);
               }
               out.writeShort(padding);
               out.write(new byte[padding]);
               break;
            }
            default:
               Object d = f.get(this);
               out.writeBoolean(d != null);
               if ( d != null ) {
                  out.writeObject(d);
               }
               //               throw new IllegalArgumentException("The field type " + fieldTypes[i] + " in class " + getClass()
               //                  + " is unsupported by util.dump.ExternalizableBean.");
            }

            if ( ft.isLengthDynamic() ) {
               out.flush();
               originalOut.writeInt(bytesCache.size());
               bytesCache.writeTo(originalOut);
               out = originalOut;
               streamCache._inUse = false;
            }
         }
      }
      catch ( Exception e ) {
         throw new RuntimeException("Failed to externalize class " + getClass().getName(), e);
      }
   }

   private boolean isCompatible( FieldType inputType, FieldType configuredType ) {
      return switch ( inputType ) {
         case Boolean, pBoolean -> switch ( configuredType ) {
            case Boolean, pBoolean -> true;
            default -> false;
         };
         case Character, pChar -> switch ( configuredType ) {
            case Character, pChar -> true;
            default -> false;
         };
         case Byte, pByte -> switch ( configuredType ) {
            case Byte, pByte -> true;
            default -> false;
         };
         case Short, pShort -> switch ( configuredType ) {
            case Short, pShort -> true;
            default -> false;
         };
         case Integer, pInt -> switch ( configuredType ) {
            case Integer, pInt -> true;
            default -> false;
         };
         case Long, pLong -> switch ( configuredType ) {
            case Long, pLong -> true;
            default -> false;
         };
         case Float, pFloat -> switch ( configuredType ) {
            case Float, pFloat -> true;
            default -> false;
         };
         case Double, pDouble -> switch ( configuredType ) {
            case Double, pDouble -> true;
            default -> false;
         };
         default -> false;
      };
   }

   /**
    * By adding this annotation to a class implementing ExternalizableBean, you can make certain, that the byte[]
    * created by externalizing has a size where <code>size%sizeModulo==0</code>, i.e. it is divisible by
    * <code>sizeModulo</code>.<br>
    * <p>
    * BEWARE: When using this annotation, the max value for @externalize index is 254, since 255 is needed for padding.
    */
   @Retention(RetentionPolicy.RUNTIME)
   @Target({ ElementType.TYPE })
   @interface externalizationPadding {

      short sizeModulo();
   }


   /**
    * By adding this annotation to a class implementing ExternalizableBean, you can make certain, that a dump with a
    * different version than the current one will not be opened. The old dump will be renamed and a new dump file
    * will be created.
    */
   @Retention(RetentionPolicy.RUNTIME)
   @Target({ ElementType.TYPE })
   @interface externalizationVersion {

      OnIncompatibleVersion onIncompatibleVersion() default OnIncompatibleVersion.RenameDump;

      int version();
   }


   /**
    * Annotating fields gives a better performance compared to methods. You can annotate even private fields.
    * If you annotate methods, it's enough to annotate either the getter or the setter. Of course you can also annotate both, but the indexes must match.
    */
   @Retention(RetentionPolicy.RUNTIME)
   @Target({ ElementType.FIELD, ElementType.METHOD })
   @interface externalize {

      /**
       * The default type of the first generic argument, like in <code>List&lt;GenericType0&gt;</code>.
       * You should set this, if the most frequent type of this container's generic argument (the List values in the example) does not match the declared generic type.
       * In that case setting this value improves both space requirement and performance.
       */
      Class defaultGenericType0() default System.class; // System.class is just a placeholder for nothing, in order to make this argument optional

      /**
       * The default type of the second generic argument, like in <code>Map&lt;K, GenericType1&gt;</code>.
       * You should set this, if the most frequent type of this container's second generic argument (the Map values in the example) does not match the declared generic type.
       * In that case setting this value improves both space requirement and performance.
       */
      Class defaultGenericType1() default System.class; // System.class is just a placeholder for nothing, in order to make this argument optional

      /**
       * The default type of this field.
       * You should set this, if the most frequent type of this field's instances does not match the declared field type.
       * In that case setting this value improves both space requirement and performance.
       */
      Class defaultType() default System.class; // System.class is just a placeholder for nothing, in order to make this argument optional

      /**
       * The value for primitive fields being migrated from boxed values, in case the latter reads null from the input.
       */
      boolean pBooleanNullValue() default false;

      /**
       * The value for primitive fields being migrated from boxed values, in case the latter reads null from the input.
       */
      byte pByteNullValue() default 0;

      /**
       * The value for primitive fields being migrated from boxed values, in case the latter reads null from the input.
       */
      char pCharNullValue() default 0;

      /**
       * The value for primitive fields being migrated from boxed values, in case the latter reads null from the input.
       */
      double pDoubleNullValue() default 0.0;

      /**
       * The value for primitive fields being migrated from boxed values, in case the latter reads null from the input.
       */
      float pFloatNullValue() default 0.0f;

      /**
       * The value for primitive fields being migrated from boxed values, in case the latter reads null from the input.
       */
      int pIntNullValue() default 0;

      /**
       * The value for primitive fields being migrated from boxed values, in case the latter reads null from the input.
       */
      long pLongNullValue() default 0L;

      /**
       * The value for primitive fields being migrated from boxed values, in case the latter reads null from the input.
       */
      short pShortNullValue() default 0;

      /**
       * Aka index. Must be unique. Convention is to start from 1. To guarantee compatibility between revisions of a bean,
       * you may never change the field type or any of the default*Types while reusing the same index specified with this parameter.
       * Doing so will corrupt old data dumps.<br>
       * <p>
       * If you need values bigger than 127, simply write (byte)200, that will work out fine.
       */
      byte value();
   }


   enum OnIncompatibleVersion {
      DeleteDump,
      RenameDump,
      RewriteDump
   }

}
