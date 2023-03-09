package util.dump;

import java.io.EOFException;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.BufferOverflowException;
import java.nio.BufferUnderflowException;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;

import javax.annotation.Nullable;

import gnu.trove.map.TLongIntMap;
import gnu.trove.map.hash.TLongIntHashMap;
import util.dump.reflection.FieldAccessor;


/**
 * Uses memory-mapped buffers for reading file contents almost instantaneously.
 * <p>
 * Initially limited to lookup and updates file sizes < 2 GiB.
 * <p>
 * Initially limited to int / Integer and long / Long field types.
 */
public class MmappedUniqueIndex<E> extends UniqueIndex<E> {

   public static final String           UNSUPPORTED_TYPE = "Initially limited to int / Integer and long / Long field types.";
   private             RandomAccessFile _lookupOutputRaf;
   private             MappedByteBuffer _lookupOutputMappedByteBuffer;
   private             RandomAccessFile _updatesOutputRaf;
   private             MappedByteBuffer _updatesOutputMappedByteBuffer;

   public MmappedUniqueIndex( Dump<E> dump, FieldAccessor fieldAccessor ) {
      super(dump, fieldAccessor);
   }

   public MmappedUniqueIndex( Dump<E> dump, String fieldName ) throws NoSuchFieldException {
      super(dump, fieldName);
   }

   @Override
   public void add( E o, long pos ) {
      try {
         if ( _fieldIsInt ) {
            int key = getIntKey(o);
            if ( _lookupInt.containsKey(key) ) {
               throw new DuplicateKeyException("Dump already contains an instance with the key " + key);
            }
            _lookupInt.put(key, pos);
            getLookupBuffer().putInt(key);
         } else if ( _fieldIsLong ) {
            long key = getLongKey(o);
            if ( _lookupLong.containsKey(key) ) {
               throw new DuplicateKeyException("Dump already contains an instance with the key " + key);
            }
            _lookupLong.put(key, pos);
            getLookupBuffer().putLong(key);
         }
         getLookupBuffer().putLong(pos);

         if ( isCompactLookupNeeded() ) {
            compactLookup();
         }
      }
      catch ( BufferOverflowException argh ) {
         throw new RuntimeException("Failed to add key to index " + getLookupFile(), argh);
      }
   }

   @Override
   public void close() throws IOException {
      closeLookupOutputBuffer();
      closeUpdatesOutputBuffer();

      super.close();
   }

   @Override
   public void flush() throws IOException {
      // flushBuffer(_lookupOutputMappedByteBuffer, _lookupOutputStreamChannel);
      // flushBuffer(_updatesOutputMappedByteBuffer, _updatesOutputStreamChannel);
   }

   @Override
   protected void addToIgnoredPositions( long pos ) {
      try {
         // we add this position to the stream of ignored positions used during load()
         getUpdatesBuffer().putLong(pos);
      }
      catch ( BufferOverflowException argh ) {
         throw new RuntimeException("Failed to append to updates file " + getUpdatesFile(), argh);
      }
   }

   @Override
   protected void init() {
      if ( !_fieldIsInt && !_fieldIsLong ) {
         throw new IllegalArgumentException(UNSUPPORTED_TYPE);
      }
      super.init();
   }

   @Override
   protected void initLookupOutputStream() {
      try {
         _lookupOutputRaf = new RandomAccessFile(getLookupFile(), "rw");
         _lookupOutputStreamChannel = _lookupOutputRaf.getChannel();
      }
      catch ( IOException argh ) {
         throw new RuntimeException("Failed to initialize dump index with lookup file " + getLookupFile(), argh);
      }
   }

   protected void initUpdatesOutputStream() {
      try {
         _updatesOutputRaf = new RandomAccessFile(getUpdatesFile(), "rw");
         _updatesOutputStreamChannel = _updatesOutputRaf.getChannel();
      }
      catch ( IOException argh ) {
         throw new RuntimeException("Failed to initialize dump index with lookup file " + getLookupFile(), argh);
      }
   }

   @Override
   protected void load() {
      if ( !getLookupFile().exists() || getLookupFile().length() == 0 ) {
         return;
      }

      final TLongIntMap positionsToIgnore;
      if ( getUpdatesFile().exists() ) {
         if ( getUpdatesFile().length() % 8 != 0 ) {
            throw new RuntimeException("Index corrupted: " + getUpdatesFile() + " has unbalanced size.");
         }
         positionsToIgnore = new TLongIntHashMap((int)(getUpdatesFile().length() / 8));
         try (FileInputStream fileInputStream = new FileInputStream(getUpdatesFile())) {
            try (FileChannel channel = fileInputStream.getChannel()) {
               MappedByteBuffer buffer = channel.map(MapMode.READ_ONLY, 0, channel.size());
               long pos;
               while ( (pos = readNextPosition(buffer)) != -1 ) {
                  positionsToIgnore.adjustOrPutValue(pos, 1, 1);
               }
            }
         }
         catch ( IOException argh ) {
            // since we do a _updatesFile.exists() this is most unlikely
            throw new RuntimeException("Failed read updates from " + getUpdatesFile(), argh);
         }
      } else {
         positionsToIgnore = new TLongIntHashMap();
      }

      boolean mayEOF = true;
      final int size = getHeadroomForLoad(getLookupSizeFromFiles());
      initLookupMap(size);
      if ( _fieldIsInt ) {
         FileChannel in = null;
         try {
            in = new FileInputStream(getLookupFile()).getChannel();
            MappedByteBuffer buffer = in.map(MapMode.READ_ONLY, 0, in.size());

            while ( true ) {
               Object payload = readPayload(buffer);
               if ( payload != null ) {
                  mayEOF = false;
               }
               int key = buffer.getInt();
               mayEOF = false;
               long pos = buffer.getLong();
               mayEOF = true;
               if ( positionsToIgnore.get(pos) > 0 ) {
                  positionsToIgnore.adjustValue(pos, -1);
                  continue;
               }
               if ( !_dump._deletedPositions.contains(pos) ) {
                  long previousEntry = _lookupInt.put(key, pos);
                  if ( previousEntry != _noEntry ) {
                     _lookupInt.put(key, previousEntry);
                     throw new DuplicateKeyException("index lookup " + getLookupFile() + " is broken - contains non unique key " + key);
                  }
                  cachePayload(pos, payload);
               }
            }
         }
         catch ( BufferUnderflowException | EOFException argh ) {
            if ( !mayEOF ) {
               throw new RuntimeException("Failed to read lookup from " + getLookupFile() + ", file is unbalanced - unexpected EoF", argh);
            }
         }
         catch ( IOException argh ) {
            throw new RuntimeException("Failed to read lookup from " + getLookupFile(), argh);
         }
         finally {
            if ( in != null ) {
               try {
                  in.close();
               }
               catch ( IOException argh ) {
                  throw new RuntimeException("Failed to close input stream.", argh);
               }
            }
         }
      } else if ( _fieldIsLong ) {
         FileChannel in = null;
         try {
            in = new FileInputStream(getLookupFile()).getChannel();
            MappedByteBuffer buffer = in.map(MapMode.READ_ONLY, 0, in.size());

            while ( true ) {
               Object payload = readPayload(buffer);
               if ( payload != null ) {
                  mayEOF = false;
               }
               long key = buffer.getLong();
               mayEOF = false;
               long pos = buffer.getLong();
               mayEOF = true;
               if ( positionsToIgnore.get(pos) > 0 ) {
                  positionsToIgnore.adjustValue(pos, -1);
                  continue;
               }
               if ( !_dump._deletedPositions.contains(pos) ) {
                  long previousEntry = _lookupLong.put(key, pos);
                  if ( previousEntry != _noEntry ) {
                     _lookupLong.put(key, previousEntry);
                     throw new DuplicateKeyException("index lookup " + getLookupFile() + " is broken - contains non unique key " + key);
                  }
                  cachePayload(pos, payload);
               }
            }
         }
         catch ( BufferUnderflowException | EOFException argh ) {
            if ( !mayEOF ) {
               throw new RuntimeException("Failed to read lookup from " + getLookupFile() + ", file is unbalanced - unexpected EoF", argh);
            }
         }
         catch ( IOException argh ) {
            throw new RuntimeException("Failed to read lookup from " + getLookupFile(), argh);
         }
         finally {
            if ( in != null ) {
               try {
                  in.close();
               }
               catch ( IOException argh ) {
                  throw new RuntimeException("Failed to close input stream.", argh);
               }
            }
         }
      }
   }

   protected long readNextPosition( MappedByteBuffer updatesInput ) {
      if ( updatesInput == null ) {
         return -1;
      }
      if ( updatesInput.remaining() < 8 ) {
         return -1;
      }

      try {
         return updatesInput.getLong();
      }
      catch ( BufferUnderflowException argh ) {
         return -1;
      }
   }

   protected Object readPayload( MappedByteBuffer in ) {
      return null;
   }

   @Override
   void delete( E o, long pos ) {
      if ( _fieldIsInt ) {
         int key = getIntKey(o);
         long p = _lookupInt.remove(key);
         if ( p != pos ) {
            _lookupInt.put(key, p);
            throw new DuplicateKeyException("Dump apparently contains two instances with the key " + key + " at positions " + p + " and " + pos);
         }
      } else if ( _fieldIsLong ) {
         long key = getLongKey(o);
         long p = _lookupLong.remove(key);
         if ( p != pos ) {
            _lookupLong.put(key, p);
            throw new DuplicateKeyException("Dump apparently contains two instances with the key " + key + " at positions " + p + " and " + pos);
         }
      }
   }

   private void closeBuffer( @Nullable MappedByteBuffer buffer, FileChannel channel ) throws IOException {
      if ( buffer != null ) {
         flushBuffer(buffer, channel);
         channel.truncate(getUsedSize(channel, buffer));
         channel.close();
      }
   }

   private void closeLookupOutputBuffer() throws IOException {
      closeBuffer(_lookupOutputMappedByteBuffer, _lookupOutputStreamChannel);
      _lookupOutputMappedByteBuffer = null;
      if ( _lookupOutputRaf != null ) {
         _lookupOutputRaf.close();
      }
   }

   private void closeUpdatesOutputBuffer() throws IOException {
      closeBuffer(_updatesOutputMappedByteBuffer, _updatesOutputStreamChannel);
      _updatesOutputMappedByteBuffer = null;
      if ( _updatesOutputRaf != null ) {
         _updatesOutputRaf.close();
      }
   }

   private void flushBuffer( @Nullable MappedByteBuffer buffer, FileChannel channel ) throws IOException {
      if ( buffer != null ) {
         buffer.force(0, buffer.position());
         channel.force(false);
      }
   }

   /**
    * Defines the number of key-value mappings or positions we should reserve when mapping the output buffers.
    */
   private long getAppendElementCount() {
      return 4 * 1024 * 1024;
   }

   /**
    * Defines the size of each key-value mapping we should reserve when mapping the output buffer.
    */
   private long getAppendMappingSize() {
      if ( _fieldIsInt ) {
         return getAppendPositionSize() + 4;  // size of primitive int
      }
      if ( _fieldIsLong ) {
         return getAppendPositionSize() + 8;  // size of primitive long
      }
      throw new IllegalArgumentException(UNSUPPORTED_TYPE);
   }

   /**
    * Defines the size of each position entry we should reserve when mapping the output buffer.
    */
   private long getAppendPositionSize() {
      return 8;  // size of primitive long
   }

   private MappedByteBuffer getLookupBuffer() {
      synchronized ( this ) {
         if ( _lookupOutputMappedByteBuffer == null || _lookupOutputMappedByteBuffer.remaining() < getAppendMappingSize() ) {
            remapLookupOutputBuffer();
         }
         return _lookupOutputMappedByteBuffer;
      }
   }

   private MappedByteBuffer getUpdatesBuffer() {
      synchronized ( this ) {
         if ( _updatesOutputStreamChannel == null ) {
            initUpdatesOutputStream();
         }

         if ( _updatesOutputMappedByteBuffer == null || _updatesOutputMappedByteBuffer.remaining() < getAppendPositionSize() ) {
            remapUpdatesOutputBuffer();
         }
         return _updatesOutputMappedByteBuffer;
      }
   }

   private long getUsedSize( FileChannel channel, MappedByteBuffer buffer ) throws IOException {
      long unusedOversize = buffer == null ? 0 : buffer.remaining();
      return channel.size() - unusedOversize;
   }

   private MappedByteBuffer remapBuffer( FileChannel channel, @Nullable MappedByteBuffer buffer, long freeSize ) throws IOException {
      flushBuffer(buffer, channel);
      long usedSize = getUsedSize(channel, buffer);
      return channel.map(MapMode.READ_WRITE, usedSize, freeSize);
   }

   private synchronized void remapLookupOutputBuffer() {
      try {
         _lookupOutputMappedByteBuffer = remapBuffer(_lookupOutputStreamChannel, _lookupOutputMappedByteBuffer,
               getAppendElementCount() * getAppendMappingSize());
      }
      catch ( IOException argh ) {
         _lookupOutputMappedByteBuffer = null;
         throw new RuntimeException("Failed to initialize dump index with lookup file " + getLookupFile(), argh);
      }
   }

   private synchronized void remapUpdatesOutputBuffer() {
      try {
         _updatesOutputMappedByteBuffer = remapBuffer(_updatesOutputStreamChannel, _updatesOutputMappedByteBuffer,
               getAppendElementCount() * getAppendPositionSize());
      }
      catch ( IOException argh ) {
         _updatesOutputMappedByteBuffer = null;
         throw new RuntimeException("Failed to init updates output buffer " + getUpdatesFile(), argh);
      }
   }
}
