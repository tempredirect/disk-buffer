package com.logicalpractice.diskbuffer;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

import static java.nio.file.StandardOpenOption.READ;
import static java.nio.file.StandardOpenOption.WRITE;

/**
 *
 */
public final class DiskBuffer implements AutoCloseable {

    enum Type {
        INITIAL,
        CONTINUATION;

        public byte index() { return (byte)ordinal(); }

        public static int metaSize(){ return 8 + 1 + 4 ; }

        public static Type fromIndex(byte index) {
            switch(index){
                case 0x0:
                    return INITIAL;
                case 0x1:
                    return CONTINUATION;
                default:
                    throw new AssertionError("unknown type index");
            }
        }
    }

    static class RecordHeader {
        private final long id;
        private final Type type;
        private final int meta;

        RecordHeader(long id, Type type, int meta) {
            this.id = id;
            this.type = type;
            this.meta = meta;
        }
        public long id() { return id; }
        public Type type() { return type; }
        public int meta() { return meta; }

        /**
         * @return zero of negative adjustment to a dataFrame index to locate the INITIAL record
         */
        public int frameAdjustment() {
            return -(type == Type.INITIAL ? 0 : meta());
        }

        public static RecordHeader fromBuffer(ByteBuffer readBuffer) {
            long id = readBuffer.getLong();
            Type type = Type.fromIndex(readBuffer.get());
            int meta = readBuffer.getInt();
            return new RecordHeader( id, type, meta );
        }
    }

    static class Position {

        private final long id;
        private final long index;

        Position(long id, long index) {
            this.id = id;
            this.index = index;
        }
        public long id() { return id; }

        /**
         * @return the dataFrame index of the initial record
         */
        public long index() { return index; }
    }

    public static DiskBuffer open( File file ) throws IOException {
        FileChannel fileChannel = FileChannel.open(file.toPath(), READ, WRITE);
        fileChannel.position( fileChannel.size() );
        return new DiskBuffer( fileChannel, 0, 2048 );
    }

    private final long start ;
    private final int frameSize ;

    private final FileChannel fileChannel;

    // these are frame readers
    private final ByteBuffer writeBuffer;
    private final ByteBuffer readBuffer;

    private final BufferAllocator allocator = new DirectBufferAllocator();

    private long frameCount = 0L;
    private long recordCount = 0L;

    // this is about the only thing that changes ... it progresses as data is appended
    private volatile Position end = new Position(0L,0L);

    private DiskBuffer( FileChannel fileChannel, long start, int frameSize) {
        this.start = start;
        this.frameSize = frameSize;
        this.fileChannel = fileChannel;
        this.writeBuffer = ByteBuffer.allocateDirect(frameSize);
        this.readBuffer = ByteBuffer.allocateDirect(frameSize);
    }

    public long start(){ return start ;}
    private long first() { return start() + 1; }

    public long end(){ return end.id(); }

    public long size(){ return end() - start;}


    /**
     * Append the readable contains of the given byte buffer
     * @param buffer required buffer
     * @return record id
     */
    public long append( ByteBuffer buffer ) throws IOException {
        int recordSize = buffer.limit() - buffer.position();

        long recordId = end() + 1L;
        int written = 0;
        long writePosition = fileChannel.position() ;
        int framesWritten = 0;

        while( written < recordSize) {
            Type type = written == 0 ? Type.INITIAL : Type.CONTINUATION ;
            int remaining = recordSize - written;
            int availableFrame = frameSize - type.metaSize();
            int toWrite = Math.min(remaining, availableFrame );
            writeBuffer.clear();
            writeBuffer.putLong(recordId);

            writeBuffer.put(type.index())
                .putInt( type == Type.INITIAL ? recordSize : framesWritten);

            // advance the limit
            buffer.limit(written + toWrite);

            writeBuffer.put(buffer);

            writeBuffer
                .flip()
                .limit(frameSize);

            int progress = 0;
            while( progress < frameSize ) {
                progress += fileChannel.write(writeBuffer);
            }

            written += toWrite;
            framesWritten ++;
        }
        fileChannel.force(false); //sync to disk

        frameCount += framesWritten;
        recordCount ++;
        // assigning this last makes the record visible via get()
        end = new Position(recordId, writePosition / frameSize);

        return recordId;
    }

    public ByteBuffer get( long id ) throws IOException {
        if( id <= start() ){
            throw new IllegalArgumentException("id <= start. Not present in this DiskBuffer");
        }
        long endId = end();
        if( id > endId ){
            throw new IllegalArgumentException("id > end. Not present in this DiskBuffer");
        }

        // special case the last record
        if( id == endId ) {
            return readRecordStarting( end.index() );
        }
        if( id == start + 1 ) { // start is the id before the first
            return readRecordStarting( 0 );
        }

        long recordIndex = id - first(); // looking for the recordIndex'th record in this buffer

        long frameIndex = recordIndex * averageFrameCount()  ;

        RecordHeader header ;

        header = readHeader( frameIndex );
        boolean found = false;
        while( !found ){
            long idDiff = header.id() - id;
            if (idDiff == 0) {
                // found the record...
                frameIndex = frameIndex + header.frameAdjustment();
                found = true;
            } else if ( idDiff < 0) {
                // miss... but by how much? locate the initial header of the found record
                long foundRecordFrameIndex = frameIndex + header.frameAdjustment();
                header = readHeader( foundRecordFrameIndex );
                assert header.type() == Type.INITIAL;

                // must move backward
                frameIndex = -(idDiff * averageFrameCount());

            } else {
                // idDiff > 0
                frameIndex = idDiff * averageFrameCount();
            }
        }

        // frameIndex will be set
        return readRecordStarting( frameIndex );
    }

    private ByteBuffer readRecordStarting(long frameIndex) throws IOException {

        RecordHeader initialHeader = readHeader(frameIndex);

        assert initialHeader.type() == Type.INITIAL;

        int dataSize = initialHeader.meta();

        ByteBuffer result = allocator.allocate(dataSize);

        int remaining = dataSize;
        int metaSize = Type.metaSize();

        while( remaining > 0 ){

            int toRead = Math.min(frameSize - metaSize, remaining );
            result.limit(toRead + result.position());
            int read = 0;
            do{
                read +=fileChannel.read( result, offsetOf(frameIndex) + read + metaSize);
            } while( read < toRead );
            remaining -= read;
            frameIndex ++;
        }
        result.flip();

        return result;

    }

    private int averageFrameCount() {
        return (int) (frameCount / recordCount) ; // todo make averageFrameCount a lot smarter
    }


    RecordHeader readHeader( long index ) throws IOException {
        long offset = offsetOf( index );
        readBuffer.clear();
        int read = 0;
        do {
            read += fileChannel.read( readBuffer, offset + read );
        } while( read < Type.INITIAL.metaSize() ); // todo the metaSize is now constant for both types

        readBuffer.flip();

        return RecordHeader.fromBuffer(readBuffer);
    }

    ByteBuffer readFrame( long index ) throws IOException {

        long offset = offsetOf(index);

        readBuffer.clear();

        int read = 0;
        while ( read < frameSize ) {
            read += fileChannel.read( readBuffer, offset + read );
        }
        readBuffer.flip();
        return readBuffer;
    }

    private long offsetOf(long index) {
        return frameSize * index;
    }

    @Override
    public void close() throws Exception {
        fileChannel.close();
    }
}
