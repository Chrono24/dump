package util.dump;

import static java.nio.ByteOrder.BIG_ENDIAN;
import static java.nio.channels.FileChannel.MapMode.READ_ONLY;
import static java.nio.channels.FileChannel.MapMode.READ_WRITE;
import static java.nio.file.StandardOpenOption.CREATE_NEW;
import static java.nio.file.StandardOpenOption.SPARSE;
import static java.nio.file.StandardOpenOption.WRITE;
import static jdk.incubator.foreign.MemoryLayout.PathElement.groupElement;
import static jdk.incubator.foreign.MemoryLayout.PathElement.sequenceElement;
import static jdk.incubator.foreign.MemoryLayout.sequenceLayout;
import static jdk.incubator.foreign.MemoryLayout.structLayout;
import static jdk.incubator.foreign.MemoryLayouts.JAVA_LONG;

import java.io.File;
import java.io.IOException;
import java.lang.invoke.VarHandle;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.EnumSet;
import java.util.Map;
import java.util.function.ToLongFunction;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gnu.trove.list.TLongList;
import gnu.trove.list.array.TLongArrayList;
import gnu.trove.map.TLongLongMap;
import gnu.trove.map.hash.TLongLongHashMap;
import jdk.incubator.foreign.GroupLayout;
import jdk.incubator.foreign.MemoryLayout;
import jdk.incubator.foreign.MemoryLayout.PathElement;
import jdk.incubator.foreign.MemorySegment;
import jdk.incubator.foreign.ResourceScope;
import util.dump.cache.LRUCache;
import util.dump.reflection.FieldAccessor;
import util.dump.reflection.FieldFieldAccessor;
import util.dump.reflection.Reflection;


/**
 * Special-purpose fixed-size off-heap id-to-pos lookup (not only) for sharded dumps.
 * <p>
 * Employs a simple array-like approach, based on the assumption that IDs either start at 1 and keep incrementing, or will be constrained to a closed range
 * defined a priori. In the former case, the backing file will keep growing, while in the latter, the bounds are set at initialization.
 * <p>
 * Requires compiler and runtime parameter --add-modules=jdk.incubator.foreign in order to work.
 */
public abstract class MmapLongIdIndex<E> extends DumpIndex<E> implements UniqueConstraint<E> {

   private static final Logger _log = LoggerFactory.getLogger(MmapLongIdIndex.class);

   private static final boolean PARANOIA_MODE = true;

   private static final VarHandle LONG_ARRAY_ACCESS = sequenceLayout(JAVA_LONG).varHandle(long.class, sequenceElement());

   public static <E> MmapLongIdIndex<E> forClosedRange( Dump<E> dump, String fieldName, long minKey, long maxKey ) throws NoSuchFieldException {
      return new ClosedRangeMmapLongIdIndex<>(dump, fieldName, minKey, maxKey);
   }

   public static <E> MmapLongIdIndex<E> forClosedRange( Dump<E> dump, FieldAccessor fieldAccessor, long minKey, long maxKey ) {
      return new ClosedRangeMmapLongIdIndex<>(dump, fieldAccessor, minKey, maxKey);
   }

   public static <E> MmapLongIdIndex<E> forOpenRange( Dump<E> dump, String fieldName, long minKey ) throws NoSuchFieldException {
      return new OpenRangeMmapLongIdIndex<>(dump, fieldName, minKey);
   }

   public static <E> MmapLongIdIndex<E> forOpenRange( Dump<E> dump, FieldAccessor fieldAccessor, long minKey ) {
      return new OpenRangeMmapLongIdIndex<>(dump, fieldAccessor, minKey);
   }

   private static boolean isPowerOfTwo( long n ) {
      return n > 0L && (n & n - 1L) == 0L;
   }

   private static long longArrayGet( MemorySegment array, long index ) {
      return (long)LONG_ARRAY_ACCESS.get(array, index);
   }

   private static long longArrayGetVolatile( MemorySegment array, long index ) {
      return (long)LONG_ARRAY_ACCESS.getVolatile(array, index);
   }

   private static void longArraySet( MemorySegment array, long index, long pos ) {
      LONG_ARRAY_ACCESS.set(array, index, pos);
   }

   private static void longArraySetVolatile( MemorySegment array, long index, long pos ) {
      LONG_ARRAY_ACCESS.setVolatile(array, index, pos);
   }

   protected final ToLongFunction<Object> _getKey;

   protected final Path _lookupPath;

   protected final long _minKey;
   protected final long _maxKey;

   private final IndexCorrections _indexCorrections = new IndexCorrections();

   protected FileLayout _fileLayout;
   private   Header     _header;

   private          MemorySegment _tableSegment;
   private volatile long          _tableCapacity;

   private MmapLongIdIndex( Dump<E> dump, String fieldName, long minKey, long maxKey ) throws NoSuchFieldException {
      this(dump, new FieldFieldAccessor(Reflection.getField(dump._beanClass, fieldName)), minKey, maxKey);
   }

   private MmapLongIdIndex( Dump<E> dump, FieldAccessor fieldAccessor, long minKey, long maxKey ) {
      super(dump, fieldAccessor, new File(dump.getDumpFile().getParentFile(), dump.getDumpFile().getName() + "." + fieldAccessor.getName() + ".mmap.lookup"));

      _lookupPath = Paths.get(getLookupFile().getPath());

      _minKey = minKey;
      _maxKey = maxKey;

      if ( _fieldIsLong ) {
         if ( _fieldIsLongObject ) {
            _getKey = o -> {
               try {
                  return (Long)_fieldAccessor.get(o);
               }
               catch ( Exception e ) {
                  throw new RuntimeException(e);
               }
            };
         } else {
            _getKey = o -> {
               try {
                  return _fieldAccessor.getLong(o);
               }
               catch ( Exception e ) {
                  throw new RuntimeException(e);
               }
            };
         }
      } else if ( _fieldIsInt ) {
         if ( _fieldIsIntObject ) {
            _getKey = o -> {
               try {
                  return (Integer)_fieldAccessor.get(o);
               }
               catch ( Exception e ) {
                  throw new RuntimeException(e);
               }
            };
         } else {
            _getKey = o -> {
               try {
                  return _fieldAccessor.getInt(o);
               }
               catch ( Exception e ) {
                  throw new RuntimeException(e);
               }
            };
         }
      } else {
         throw new IllegalStateException("only long (and int) fields implemented");
      }
   }

   @Override
   public void close() throws IOException {
      _log.info("{} closing...", _lookupPath.getFileName());
      flushTable();
      closeHeader();

      super.close();
      _log.info("{} closed.", _lookupPath.getFileName());
   }

   @Override
   public boolean contains( int key ) {
      return contains((long)key);
   }

   @Override
   public boolean contains( long key ) {
      long index = indexFor(key);
      long pos = getPosAt(index);

      if ( pos < 0 ) {
         return false;
      }

      synchronized ( _dump ) {
         return !_dump._deletedPositions.contains(pos);
      }
   }

   @Override
   public boolean contains( Object key ) {
      if ( key instanceof Long l ) {
         return contains(l.longValue());
      }
      if ( key instanceof Integer i ) {
         return contains(i.intValue());
      }

      throw new IllegalArgumentException("only long (and int) fields implemented");
   }

   @Override
   public void flush() throws IOException {
      // We don't have to do anything to push data out-of-process, it's already in the page cache. Hammering the file to disk all the time gets us nowhere.
   }

   @Override
   public long[] getAllLongKeys() {
      TLongList keys = new TLongArrayList(getNumKeys(), -1);
      MemorySegment table = _tableSegment.asReadOnly();

      for ( long index = 0, n = capacity(table); index < n; ++index ) {
         // not used during live operation, hence concurrency is not an issue
         if ( getPosAt(table, index) >= 0 ) {
            keys.add(keyOffsetRevert(index));
         }
      }

      return keys.toArray();
   }

   @Override
   public TLongList getAllPositions() {
      throw new UnsupportedOperationException();
   }

   @Override
   public Object getKey( E o ) {
      return keyFor(o);
   }

   @Override
   public int getNumKeys() {
      return (int)_header.getNumKeys();
   }

   @Override
   public E lookup( int key ) {
      return lookup((long)key);
   }

   @Override
   public E lookup( long key ) {
      long index = indexFor(key);
      long pos = getPosAt(index);

      if ( pos < 0 ) {
         return null;
      }

      synchronized ( _dump ) {
         return !_dump._deletedPositions.contains(pos) ? _dump.get(pos) : null;
      }
   }

   @Override
   public E lookup( Object key ) {
      if ( key instanceof Long l ) {
         return lookup(l.longValue());
      }
      if ( key instanceof Integer i ) {
         return lookup(i.longValue());
      }

      throw new IllegalArgumentException("only long (and int) fields implemented");
   }

   protected long capacity( MemorySegment segment ) {
      return segment.byteSize() / Long.BYTES;
   }

   @Override
   protected boolean checkMeta() {
      return super.checkMeta() && checkHeader();
   }

   protected boolean checkNumKeys( Header header ) {
      if ( header.getNumKeys() < 0 ) {
         return false;
      }
      if ( header.getNumKeys() == 0 && _dump.getDumpSize() > 0 ) {
         return false;
      }

      return header.getNumKeys() <= header.getTableBytes() / Long.BYTES;
   }

   @Override
   protected String getIndexType() {
      return MmapLongIdIndex.class.getSimpleName();
   }

   protected void growTableSegment( long minTableSize ) {
      long tableOffset = _header.getTableOffset();
      long tableSize = _header.getTableBytes();

      long currentFileSize = tableOffset + tableSize;
      long minFileSize = tableOffset + minTableSize;

      if ( minFileSize <= currentFileSize ) {
         throw new IllegalArgumentException("no need to grow table if min size <= current size");
      }

      try {
         long alignedFileSize = minFileSize + (-minFileSize & (_fileLayout.blockSize() - 1));

         _header.setTableBytes(alignedFileSize - tableOffset);
         mapTableSegment();

         _log.info("{} grew from {} to {} bytes", _lookupPath.getFileName(), currentFileSize, alignedFileSize);
      }
      catch ( IOException e ) {
         throw new RuntimeException(e);
      }
   }

   @Override
   protected void initLookupMap() {
      // does nothing
   }

   @Override
   protected void initLookupOutputStream() {
      try {
         if ( Files.exists(_lookupPath) ) {
            openExisting();
         } else {
            createNew();
         }
      }
      catch ( IOException e ) {
         throw new RuntimeException(e);
      }
   }

   protected abstract long initialTableSize();

   @Override
   protected void load() {
      // does nothing, we only need to open the mmapped file
   }

   protected abstract void setPosAt( long index, long pos );

   protected final void setPosAtVolatile( long index, long pos ) {
      longArraySetVolatile(_tableSegment, index, posOffsetApply(pos));
   }

   protected long tableCapacity() {
      return _tableCapacity;
   }

   @Override
   void add( E elem, long pos ) {
      if ( add0(elem, pos) ) {
         _header.incrementNumKeys(1);
      }
   }

   @Override
   void delete( E elem, long pos ) {
      if ( delete0(elem) ) {
         _header.incrementNumKeys(-1);
      }
   }

   @Override
   boolean isUpdatable( E oldItem, E newItem ) {
      return true;
   }

   @Override
   void update( long pos, E oldElem, E newElem ) {
      if ( keyFor(oldElem) == keyFor(newElem) ) {
         return; // pos and key are identical => no change
      }

      boolean deleted = delete0(oldElem);
      boolean added = add0(newElem, pos);

      if ( added == deleted ) {
         return; // key count unchanged
      }

      if ( added ) {
         _header.incrementNumKeys(1);
      } else { // deleted
         _header.incrementNumKeys(-1);
      }
   }

   private boolean add0( E elem, long pos ) {
      long index = indexFor(elem);
      boolean unique = getPosAt(index) < 0;
      if ( !unique ) {
         throw new DuplicateKeyException("Dump already contains an instance with the key " + keyFor(elem));
      }

      setPosAt(index, pos);
      return true;
   }

   private void applyHeaderCorrections() {
      if ( _indexCorrections.numKeys != null ) {
         _log.info("{} fixing header...", _lookupPath.getFileName());
         _header.setNumKeys(_indexCorrections.numKeys);
      }
   }

   private void applyTableCorrections() {
      if ( _indexCorrections.rawContentCorrections != null ) {
         _log.info("{} fixing table...", _lookupPath.getFileName());
         _indexCorrections.rawContentCorrections.forEachEntry(( index, posInfo ) -> {
            longArraySet(_tableSegment, index, posInfo);
            return true;
         });

         if ( _header.getNumKeys() != getAllLongKeys().length ) {
            throw new IllegalStateException(_lookupPath.getFileName() + " inconsistencies post-fixup, wtf");
         }
      }
   }

   private boolean checkConsistency( Header header, MemorySegment tableSegment ) throws IOException {
      return new ConsistencyCheck(header, tableSegment).perform();
   }

   private boolean checkHeader() {
      if ( !Files.exists(_lookupPath) ) {
         return false;
      }

      try (ResourceScope autoClosedScope = ResourceScope.newConfinedScope()) {
         long fileSize = Files.size(_lookupPath);
         if ( fileSize < LeadIn.byteSize() ) {
            _log.warn("{} file is too small to contain even the header. Will rebuild index.", _lookupPath.getFileName());
            return false;
         }

         LeadIn leadIn = new LeadIn(MemorySegment.mapFile(_lookupPath, 0, LeadIn.byteSize(), READ_ONLY, autoClosedScope));

         if ( leadIn.getFileMagic() != LeadIn.FILE_MAGIC ) {
            _log.warn("{} has wrong file magic. Will rebuild index.", _lookupPath.getFileName());
            return false;
         }

         boolean checkRequired = false;

         // version or size unset, unknown, or mismatching
         if ( !Header.isReadable(leadIn.getLayoutVersion()) ) {
            _log.warn("{} has unknown header version {}. Will rebuild index.", _lookupPath.getFileName(), leadIn.getLayoutVersion());
            return false;
         }

         // pin everything to current one-and-only version
         if ( leadIn.getLayoutVersion() != Header.getCurrentLayout().layoutVersion() ) {
            _log.warn("{} has mismatching header version. Will rebuild index.", _lookupPath.getFileName());
            return false;
         }

         FileLayout fileLayout = Header.layoutByVersion(leadIn.getLayoutVersion());

         if ( leadIn.getHeaderBytes() < fileLayout.headerBytes() ) {
            _log.warn("{} has mismatching header size information. Will rebuild index.", _lookupPath.getFileName());
            return false;
         }

         Header header = new Header(fileLayout, MemorySegment.mapFile(_lookupPath, 0, leadIn.getHeaderBytes(), READ_ONLY, autoClosedScope));

         if ( header.getTableOffset() <= 0 || !isPowerOfTwo(header.getTableOffset()) ) {
            _log.warn("{} has inconsistent alignment information. Will rebuild index.", _lookupPath.getFileName());
            return false;
         }
         // file size mismatch
         if ( header.getTableOffset() + header.getTableBytes() != fileSize ) {
            _log.warn("{} has mismatching file size information. Will rebuild index.", _lookupPath.getFileName());
            return false;
         }

         // configuration changes
         if ( header.getMinKey() != _minKey || header.getMaxKey() != _maxKey ) {
            _log.warn("{} has mismatching key bounds. Will rebuild index.", _lookupPath.getFileName());
            return false;
         }

         // not closed properly, or inconsistency between dump file and header state
         if ( header.getOpenedTimestamp() >= header.getClosedTimestamp() ) {
            _log.info("{} was not closed properly, checking consistency...", _lookupPath.getFileName());
            checkRequired = true;
         }

         // plausibility checks
         if ( !checkNumKeys(header) ) {
            _log.info("{} has stored implausible numKeys, checking consistency...", _lookupPath.getFileName());
            checkRequired = true;
         }

         if ( PARANOIA_MODE && !checkRequired ) {
            _log.info("{} hardcoded paranoia mode enabled, checking consistency...", _lookupPath.getFileName());
            checkRequired = true;
         }

         if ( checkRequired ) {
            MemorySegment tableSegment = MemorySegment.mapFile(_lookupPath, header.getTableOffset(), header.getTableBytes(), READ_ONLY, autoClosedScope);
            if ( !checkConsistency(header, tableSegment) ) {
               return false;
            }
         }

         _fileLayout = fileLayout; // store this, so we won't have to restart at the lead-in

         return true;
      }
      catch ( Exception argh ) {
         throw new RuntimeException(argh);
      }

   }

   private void closeHeader() {
      if ( _header != null ) {
         _header.setClosedTimestamp(System.currentTimeMillis());
         _header.flush();
      }
   }

   private void createNew() throws IOException {
      Files.newByteChannel(_lookupPath, EnumSet.of(CREATE_NEW, SPARSE, WRITE)).close(); // just create sparse file; MemorySegment API takes a Path as of java 17
      _fileLayout = Header.getCurrentLayout();

      initHeader(mapHeaderSegment());

      mapTableSegment();
   }

   private boolean delete0( E elem ) {
      long index = indexFor(elem);
      boolean deleted = getPosAt(index) >= 0;
      setPosAt(index, -1);
      return deleted;
   }

   private void flushTable() {
      if ( _tableSegment != null ) {
         _tableSegment.force();
      }
   }

   private long getPosAt( long index ) {
      if ( index < 0 ) {
         throw new IndexOutOfBoundsException("index out of bounds");
      }

      if ( index < tableCapacity() ) {
         return getPosAtVolatile(_tableSegment, index);
      } else {
         return -1;
      }
   }

   private long getPosAt( MemorySegment tableSegment, long index ) {
      return posOffsetRevert(longArrayGet(tableSegment, index));
   }

   private long getPosAtVolatile( MemorySegment tableSegment, long index ) {
      return posOffsetRevert(longArrayGetVolatile(tableSegment, index));
   }

   private long indexFor( long key ) {
      if ( _minKey <= key && key <= _maxKey ) {
         return keyOffsetApply(key);
      }

      throw new IndexOutOfBoundsException("key out of bounds");
   }

   private long indexFor( E elem ) {
      long key = keyFor(elem);
      return indexFor(key);
   }

   private void initHeader( MemorySegment headerSegment ) {
      _header = new Header(_fileLayout, headerSegment);

      _header.setFileMagic(LeadIn.FILE_MAGIC);
      _header.setLayoutVersion(_fileLayout.layoutVersion());
      _header.setHeaderBytes(_fileLayout.headerBytes());

      _header.setTableOffset(_fileLayout.tableOffset());

      _header.setMinKey(_minKey);
      _header.setMaxKey(_maxKey);

      _header.setOpenedTimestamp(System.currentTimeMillis());
      _header.setClosedTimestamp(0L);

      _header.setTableBytes(initialTableSize());

      _header.setNumKeys(0);
   }

   private long keyFor( E elem ) {
      return _getKey.applyAsLong(elem);
   }

   /**
    * Key offset shifts the start of the array closer to the start of the actually used key range. With sharded dumps in particular, the exact key range is
    * known a priori and can be neatly catered for.
    *
    * @param realKey the actual key identifying the instance in the dump
    * @return the index into the array where the corresponding dump position is to be stored
    */
   private long keyOffsetApply( long realKey ) {
      return realKey - _minKey; // => array index
   }

   /**
    * @param arrayIndex the index into the array where the corresponding dump position is stored
    * @return the actual key identifying the instance in the dump
    * @see #keyOffsetApply(long)
    */
   private long keyOffsetRevert( long arrayIndex ) {
      return arrayIndex + _minKey; // real key
   }

   private MemorySegment mapHeaderSegment() throws IOException {
      return MemorySegment.mapFile(_lookupPath, 0, _fileLayout.headerBytes(), READ_WRITE, ResourceScope.newImplicitScope());
   }

   private void mapTableSegment() throws IOException {
      _tableSegment = MemorySegment.mapFile(_lookupPath, _header.getTableOffset(), _header.getTableBytes(), READ_WRITE, ResourceScope.newImplicitScope());
      _tableCapacity = capacity(_tableSegment);
   }

   private void openExisting() throws IOException {
      openHeader(mapHeaderSegment());

      mapTableSegment();

      applyTableCorrections();
   }

   private void openHeader( MemorySegment headerSegment ) {
      _header = new Header(_fileLayout, headerSegment);
      _header.setOpenedTimestamp(System.currentTimeMillis());

      applyHeaderCorrections();
   }

   /**
    * Pos offset masks the fact that valid dump positions start at 0, which would collide with "empty value". Pre-filling with -1 is detrimental to
    * performance, causing needless write load on initialization / segment growth, and conflicting with the idea of sparse files.
    *
    * @param realDumpPosition the actual position in the dump
    * @return the position info to be stored in the index
    */
   private long posOffsetApply( long realDumpPosition ) {
      return realDumpPosition + 1; // => stored position info
   }

   /**
    * @param storedPositioninfo the position info that is stored in the index
    * @return the actual position in the dump
    * @see #posOffsetApply(long)
    */
   private long posOffsetRevert( long storedPositioninfo ) {
      return storedPositioninfo - 1; // => real dump position
   }

   public interface Arch {

      long cacheLineBytes();

      long pageSizeBytes();

      final class AmdZen implements Arch {

         public static final Arch INSTANCE = new AmdZen();

         @Override
         public long cacheLineBytes() {
            return 64;
         }

         @Override
         public long pageSizeBytes() {
            return 4096;
         }
      }
   }


   protected record FileLayout(long layoutVersion, Arch arch, long tableOffsetInPages, long blockSizeInPages, GroupLayout headerLayout) {

      public FileLayout sanityCheck() {
         if ( tableOffset() < headerBytes() ) {
            throw new IllegalStateException("table overlaps header");
         }
         if ( blockSize() < tableOffset() ) {
            throw new IllegalStateException("table offset well exceeds first block");
         }
         return this;
      }

      long blockSize() {
         return blockSizeInPages * arch.pageSizeBytes();
      }

      long headerBytes() {
         return headerLayout.byteSize();
      }

      long tableOffset() {
         return tableOffsetInPages * arch.pageSizeBytes();
      }
   }


   protected static final class Header {

      private static final Map<Long, FileLayout> FILE_LAYOUT_BY_VERSION;

      private static final FileLayout FILE_LAYOUT_V1 = new FileLayout(1, Arch.AmdZen.INSTANCE, 1, 1024, structLayout( //

            // these are mostly constant, might change when file layout version is updated / migrated

            LeadIn.LAYOUT, //

            JAVA_LONG.withName("tableOffset"), //

            JAVA_LONG.withName("minKey"), //
            JAVA_LONG.withName("maxKey"), //

            MemoryLayout.paddingLayout(Arch.AmdZen.INSTANCE.cacheLineBytes()), // keep things cache-line-aligned

            // these change during open/close

            JAVA_LONG.withName("openedTimestamp"), //
            JAVA_LONG.withName("closedTimestamp"), //

            MemoryLayout.paddingLayout(Arch.AmdZen.INSTANCE.cacheLineBytes()), // keep things cache-line-aligned

            // changes whenever file needs to grow
            JAVA_LONG.withName("tableBytes"), //

            MemoryLayout.paddingLayout(Arch.AmdZen.INSTANCE.cacheLineBytes()), // keep things cache-line-aligned

            // changes whenever keys are added/removed
            JAVA_LONG.withName("numKeys"), //

            MemoryLayout.paddingLayout(Arch.AmdZen.INSTANCE.cacheLineBytes()) // keep things cache-line-aligned
      ).withBitAlignment(8 * Arch.AmdZen.INSTANCE.cacheLineBytes()).withName("headerLayoutV1")) //
            .sanityCheck();

      static {
         FILE_LAYOUT_BY_VERSION = Map.of(1L, FILE_LAYOUT_V1);
      }

      public static FileLayout getCurrentLayout() {
         return Header.layoutByVersion(1L);
      }

      public static boolean isReadable( long version ) {
         return FILE_LAYOUT_BY_VERSION.containsKey(version);

      }

      public static FileLayout layoutByVersion( long version ) {
         return FILE_LAYOUT_BY_VERSION.get(version);
      }

      private final MemorySegment _memorySegment;

      private final VarHandle _fileMagic;
      private final VarHandle _layoutVersion;
      private final VarHandle _headerBytes;

      private final VarHandle _tableOffset;

      private final VarHandle _minKey;
      private final VarHandle _maxKey;

      private final VarHandle _openedTimestamp;
      private final VarHandle _closedTimestamp;

      private final VarHandle _tableBytes;

      private final VarHandle _numKeys;

      public Header( FileLayout fileLayout, MemorySegment memorySegment ) {

         GroupLayout layout = fileLayout.headerLayout();

         _memorySegment = memorySegment;

         PathElement leadIn = groupElement("leadIn");
         _fileMagic = layout.varHandle(long.class, leadIn, groupElement("fileMagic"));
         _layoutVersion = layout.varHandle(long.class, leadIn, groupElement("layoutVersion"));
         _headerBytes = layout.varHandle(long.class, leadIn, groupElement("headerBytes"));

         _tableOffset = layout.varHandle(long.class, groupElement("tableOffset"));

         _minKey = layout.varHandle(long.class, groupElement("minKey"));
         _maxKey = layout.varHandle(long.class, groupElement("maxKey"));

         _openedTimestamp = layout.varHandle(long.class, groupElement("openedTimestamp"));
         _closedTimestamp = layout.varHandle(long.class, groupElement("closedTimestamp"));

         _tableBytes = layout.varHandle(long.class, groupElement("tableBytes"));

         _numKeys = layout.varHandle(long.class, groupElement("numKeys"));
      }

      public void flush() {
         _memorySegment.force();
      }

      public long getClosedTimestamp() {
         return getVolatile(_closedTimestamp);
      }

      public long getMaxKey() {
         return getVolatile(_maxKey);
      }

      public long getMinKey() {
         return getVolatile(_minKey);
      }

      public long getNumKeys() {
         return getVolatile(_numKeys);
      }

      public long getOpenedTimestamp() {
         return getVolatile(_openedTimestamp);
      }

      public long getTableBytes() {
         return getVolatile(_tableBytes);
      }

      public long getTableOffset() {
         return getVolatile(_tableOffset);
      }

      public void incrementNumKeys( long difference ) {
         _numKeys.getAndAdd(_memorySegment, difference);
      }

      public void setClosedTimestamp( long closedTimestamp ) {
         setVolatile(_closedTimestamp, closedTimestamp);
      }

      public void setFileMagic( long fileMagic ) {
         setVolatile(_fileMagic, fileMagic);
      }

      public void setHeaderBytes( long headerBytes ) {
         setVolatile(_headerBytes, headerBytes);
      }

      public void setLayoutVersion( long layoutVersion ) {
         setVolatile(_layoutVersion, layoutVersion);
      }

      public void setMaxKey( long maxKey ) {
         setVolatile(_maxKey, maxKey);
      }

      public void setMinKey( long minKey ) {
         setVolatile(_minKey, minKey);
      }

      public void setNumKeys( long numKeys ) {
         setVolatile(_numKeys, numKeys);
      }

      public void setOpenedTimestamp( long openedTimestamp ) {
         setVolatile(_openedTimestamp, openedTimestamp);
      }

      public void setTableBytes( long tableBytes ) {
         setVolatile(_tableBytes, tableBytes);
      }

      public void setTableOffset( long tableOffset ) {
         setVolatile(_tableOffset, tableOffset);
      }

      private long getVolatile( VarHandle varHandle ) {
         return (long)varHandle.getVolatile(_memorySegment);
      }

      private void setVolatile( VarHandle varHandle, long value ) {
         varHandle.setVolatile(_memorySegment, value);
      }
   }


   static final class ClosedRangeMmapLongIdIndex<E> extends MmapLongIdIndex<E> {

      private static long maxNumKeys( long minKey, long maxKey ) {
         return maxKey - minKey + 1;
      }

      private ClosedRangeMmapLongIdIndex( Dump<E> dump, String fieldName, long minKey, long maxKey ) throws NoSuchFieldException {
         super(dump, fieldName, minKey, maxKey);
         init();
      }

      private ClosedRangeMmapLongIdIndex( Dump<E> dump, FieldAccessor fieldAccessor, long minKey, long maxKey ) {
         super(dump, fieldAccessor, minKey, maxKey);
         init();
      }

      @Override
      protected long initialTableSize() {
         return maxNumKeys(_minKey, _maxKey) * Long.BYTES;
      }

      @Override
      protected void setPosAt( long index, long pos ) {
         if ( index < 0 || index >= tableCapacity() ) {
            throw new IndexOutOfBoundsException("index out of bounds");
         }

         setPosAtVolatile(index, pos);
      }
   }


   static final class OpenRangeMmapLongIdIndex<E> extends MmapLongIdIndex<E> {

      private static long deriveMaxKeyFrom( long minKey ) {
         // this basically rotates the zero-index of the array, just like simple index addition/subtraction does during offset compensation
         // return Long.MAX_VALUE + minKey; // this totally screws up the < comparison
         return minKey < 0 ? Long.MAX_VALUE + minKey : Long.MAX_VALUE;
      }

      private final Object _growLock = new Object();

      private OpenRangeMmapLongIdIndex( Dump<E> dump, String fieldName, long minKey ) throws NoSuchFieldException {
         super(dump, fieldName, minKey, deriveMaxKeyFrom(minKey));
         init();
      }

      private OpenRangeMmapLongIdIndex( Dump<E> dump, FieldAccessor fieldAccessor, long minKey ) {
         super(dump, fieldAccessor, minKey, deriveMaxKeyFrom(minKey));
         init();
      }

      protected long initialTableSize() {
         return _fileLayout.blockSize() - _fileLayout.tableOffset(); // align everything to block boundaries
      }

      @Override
      protected void setPosAt( long index, long pos ) {
         if ( index < 0 ) {
            throw new IndexOutOfBoundsException("index out of bounds");
         }

         ensureTableContains(index);
         setPosAtVolatile(index, pos);
      }

      private void ensureTableContains( long index ) {
         if ( index >= tableCapacity() ) {
            synchronized ( _growLock ) {
               if ( index >= tableCapacity() ) {
                  // fence post problem: we index the start, but segments need to cover everything to the end; hence +1
                  growTableSegment((index + 1) * Long.BYTES);
               }
            }
         }
      }
   }


   private static final class IndexCorrections {

      Long numKeys;

      TLongLongMap rawContentCorrections; // no key or pos offsets here, just write values to array indexes verbatim

      TLongLongMap rawContentCorrections() {
         if ( rawContentCorrections == null ) {
            rawContentCorrections = new TLongLongHashMap();
         }
         return rawContentCorrections;
      }
   }


   /**
    * Helper for reading in the bare minimum from existing files.
    */
   private static final class LeadIn {

      public static final long FILE_MAGIC = 0x6861696C756C6672L;

      private static final GroupLayout LAYOUT = structLayout( //

            JAVA_LONG.withName("fileMagic").withOrder(BIG_ENDIAN), //
            JAVA_LONG.withName("layoutVersion"), //
            JAVA_LONG.withName("headerBytes") //

      ).withBitAlignment(8 * Arch.AmdZen.INSTANCE.cacheLineBytes()).withName("leadIn");

      private static final VarHandle _fileMagic     = LAYOUT.varHandle(long.class, groupElement("fileMagic"));
      private static final VarHandle _layoutVersion = LAYOUT.varHandle(long.class, groupElement("layoutVersion"));
      private static final VarHandle _headerBytes   = LAYOUT.varHandle(long.class, groupElement("headerBytes"));

      public static long byteSize() {
         return LAYOUT.byteSize(); // minimum read length: header version and bytes
      }

      private final MemorySegment _memorySegment;

      public LeadIn( MemorySegment memorySegment ) {
         _memorySegment = memorySegment;
      }

      public long getFileMagic() {
         return (Long)_fileMagic.get(_memorySegment);
      }

      public long getHeaderBytes() {
         return (Long)_headerBytes.get(_memorySegment);
      }

      public long getLayoutVersion() {
         return (Long)_layoutVersion.get(_memorySegment);
      }

   }


   private final class ConsistencyCheck {

      private final Header        _header;
      private final MemorySegment _tableSegment;
      private final long          _segmentCapacity;

      private boolean _consistent = true;

      private long _numKeysInIndex;

      public ConsistencyCheck( Header header, MemorySegment tableSegment ) {
         _header = header;
         _tableSegment = tableSegment;
         _segmentCapacity = capacity(_tableSegment);
      }

      public boolean perform() throws IOException {
         long start = System.nanoTime();

         preloadSegment();
         checkNumKeys();

         if ( PARANOIA_MODE ) {
            Map<Long, byte[]> originalCache = _dump._cache;
            if ( originalCache != null ) { // otherwise things are uninitialized and lead to NPEs
               _dump.setCache(new LRUCache<>((int)_numKeysInIndex * 2, 0.5f)); // temporarily increase cache
            }
            try (ResourceScope scope = ResourceScope.newConfinedScope()) {
               MemorySegment dumpSegment = preloadDump(_dump, scope);

               checkDumpElements();
               checkIndexElements();

               _log.info("{} releasing preload mapping of size {}", _lookupPath.getFileName(), dumpSegment.byteSize());
            }
            finally {
               _dump.setCache(originalCache); // restore original configuration
            }
         }

         Duration duration = Duration.ofNanos(System.nanoTime() - start);
         if ( _consistent ) {
            _log.info("{} was checked and found to be {} in {}", _lookupPath.getFileName(), "consistent", duration);
         } else {
            _log.warn("{} was checked and found to be {} in {}", _lookupPath.getFileName(), "inconsistent", duration);
         }

         return _consistent;
      }

      private void checkDumpElements() {
         long start = System.nanoTime();
         long numKeysInDump = 0;

         long maxKey = _header.getMaxKey();
         DumpIterator<E> iterator = _dump.iterator();
         while ( iterator.hasNext() ) {
            ++numKeysInDump;

            E element = iterator.next();

            long elementKey = keyFor(element);
            long elementIndex = keyOffsetApply(elementKey); // intentionally not bounds-checked

            // not used during live operation, hence concurrency is not an issue
            long posInIndex = getPosAt(_tableSegment, elementIndex);
            long posInDump = iterator.getPosition();

            if ( elementKey > maxKey ) {
               _consistent = false;
               _log.warn("{} Dump contains element with key {}, but upper bound for key is {}! Will rebuild index.", _lookupPath.getFileName(), elementKey,
                     maxKey);
            } else if ( elementIndex >= _segmentCapacity ) {
               _log.info(
                     "{} Dump contains element with key {}, but segment capacity is {}. Has probably been added to dump already, but not yet to index; fixing.",
                     _lookupPath.getFileName(), elementKey, _segmentCapacity);
               _indexCorrections.rawContentCorrections().put(keyOffsetApply(elementKey), posOffsetApply(posInDump));
            }

            if ( posInIndex != posInDump ) {
               _consistent = false;
               _log.warn("{} Index claims element with key {} to be at position {}, but dump insists on {}! Will rebuild index.", _lookupPath.getFileName(),
                     elementKey, posInIndex, posInDump);
            }
         }
         if ( _numKeysInIndex != numKeysInDump ) {
            _log.warn("{} numKeys differ between index ({}) and dump ({}), fixing.", _lookupPath.getFileName(), _numKeysInIndex, numKeysInDump);
            _indexCorrections.numKeys = numKeysInDump;
         }

         Duration duration = Duration.ofNanos(System.nanoTime() - start);
         _log.info("{} was checked against dump iteration in {}", _lookupPath.getFileName(), duration);
      }

      private void checkIndexElements() {
         long start = System.nanoTime();
         for ( long arrayIndex = 0; arrayIndex < _segmentCapacity; ++arrayIndex ) {
            // not used during live operation, hence concurrency is not an issue
            long position = getPosAt(_tableSegment, arrayIndex);
            if ( position >= 0 ) {
               try {
                  E element = _dump.get(position);

                  if ( element == null ) {
                     _consistent = false;
                     _log.error("{} This is weird! Found position {} not to be deleted, but dump still returns null! Will rebuild index.",
                           _lookupPath.getFileName(), position);
                  } else {
                     long arrayKey = keyOffsetRevert(arrayIndex);

                     long elementKey = keyFor(element);
                     long elementIndex = keyOffsetApply(elementKey); // intentionally not bounds-checked

                     if ( elementIndex != arrayIndex ) {
                        _consistent = false;
                        _log.warn(
                              "{} Found position {} for key {} at index {}, but corresponding element from dump with key {} belongs at index {}! Will rebuild index.",
                              _lookupPath.getFileName(), position, arrayKey, arrayIndex, elementKey, elementIndex);
                     }
                  }
               }
               catch ( Exception argh ) {
                  _consistent = false;
                  _log.warn("{} Caught exception trying to get element at pos {} from dump! Will rebuild index.", _lookupPath.getFileName(), position, argh);
               }
            }
         }

         Duration duration = Duration.ofNanos(System.nanoTime() - start);
         _log.info("{} was checked against dump lookups in {}", _lookupPath.getFileName(), duration);
      }

      private void checkNumKeys() {
         long start = System.nanoTime();
         long numKeysInIndex = 0;

         long dumpSize = _dump.getDumpSize();
         for ( long arrayIndex = 0; arrayIndex < _segmentCapacity; ++arrayIndex ) {
            long position = getPosAt(_tableSegment, arrayIndex);
            if ( position >= 0 ) {
               ++numKeysInIndex;

               if ( position >= dumpSize ) {
                  _consistent = false;
                  _log.warn("{} Found position {} beyond the end of the dump file with size {}! Will rebuild index.", _lookupPath.getFileName(), position,
                        dumpSize);
               }

               if ( _dump._deletedPositions.contains(position) ) {
                  _log.warn("{} Found deleted position {} at index {}. Has probably been deleted from dump already, but not yet from index; fixing.",
                        _lookupPath.getFileName(), position, arrayIndex);
                  _indexCorrections.rawContentCorrections().put(arrayIndex, 0);
               }
            }
         }
         if ( _header.getNumKeys() != numKeysInIndex ) {
            _log.info("{} numKeys differ between caching header ({}) and bare count in actual table ({}), fixing.", _lookupPath.getFileName(),
                  _header.getNumKeys(), numKeysInIndex);
            _indexCorrections.numKeys = numKeysInIndex;
         }

         _numKeysInIndex = numKeysInIndex;
         Duration duration = Duration.ofNanos(System.nanoTime() - start);
         _log.info("{} had its contents checked in {}", _lookupPath.getFileName(), duration);
      }

      private MemorySegment preloadDump( Dump<E> dump, ResourceScope scope ) throws IOException {
         long start = System.nanoTime();

         Path dumpPath = Paths.get(dump._dumpFile.getPath());
         long dumpSize = dump.getDumpSize();
         MemorySegment dumpSegment = MemorySegment.mapFile(dumpPath, 0, dumpSize, READ_ONLY, scope);
         dumpSegment.load(); // force-fetch into memory

         Duration duration = Duration.ofNanos(System.nanoTime() - start);
         _log.info("{} was mapped and preloaded in {}", dumpPath.getFileName(), duration);
         return dumpSegment;
      }

      private void preloadSegment() {
         long start = System.nanoTime();

         _tableSegment.load(); // force-fetch into memory

         Duration duration = Duration.ofNanos(System.nanoTime() - start);
         _log.info("{} was preloaded in {}", _lookupPath.getFileName(), duration);
      }
   }
}
