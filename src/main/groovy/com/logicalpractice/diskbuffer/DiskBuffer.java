package com.logicalpractice.diskbuffer;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Path;

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

        @Override
        public String toString() {
            return "RecordHeader{" +
                    "id=" + id +
                    ", type=" + type +
                    ", meta=" + meta +
                    "} " + super.toString();
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

    public static DiskBuffer open( Path path ) throws IOException {
        DataFrameBuffer dfb = DataFrameBuffer.open(path);
        return new DiskBuffer(dfb , dfb.getFrameSize());
    }

    private final long start ;
    private final int frameSize ;

    private final DataFrameBuffer dfb;

    private final BufferAllocator allocator = new DirectBufferAllocator();

    private long frameCount = 0L;
    private long recordCount = 0L;

    // this is about the only thing that changes ... it progresses as data is appended
    private volatile Position end = new Position(0L,0L);

    private DiskBuffer( DataFrameBuffer dfb, int frameSize) {
        this.frameSize = frameSize;
        this.dfb = dfb;
        // FIXME work out if start still makes any sense
        this.start = 0;
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
        int recordSize = buffer.remaining();

        long recordId = end() + 1L;
        long recordPosition = dfb.getFrameCount();

        ByteBuffer [] frames = frameBuffersFor(recordSize);
        int availableFrame = frameSize - Type.metaSize();

        // prepare frames
        for (int i = 0; i < frames.length; i++) {
            ByteBuffer buff = frames[i];
            Type type = i == 0 ? Type.INITIAL : Type.CONTINUATION ;
            int toWrite = Math.min(buffer.remaining(), availableFrame );
            buff.putLong(recordId);

            buff.put(type.index())
                .putInt( type == Type.INITIAL ? recordSize : i);

            ByteBuffer view = buffer.duplicate();
            view.position(i * availableFrame).limit(toWrite);
            buff.put(view);
            buff.position(0);
        }

        dfb.append(frames);

        frameCount += frames.length;
        recordCount ++;

        // assigning this last makes the record visible via get()
        end = new Position(recordId, recordPosition);

        return recordId;
    }

    private ByteBuffer [] frameBuffersFor(int recordSize) {
        int frameUsageSize = frameSize - Type.metaSize();
        int required = recordSize / frameUsageSize + 1;

        ByteBuffer [] buffers = new ByteBuffer[required];

        // possible optimisation is to allocate a single buffer and split into views
        for (int i = 0; i < required; i++) {
            buffers[i] = allocator.allocate(frameSize);
        }
        return buffers;
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

        ByteBuffer frame = dfb.get(frameIndex);
        RecordHeader initialHeader = RecordHeader.fromBuffer(frame);

        assert initialHeader.type() == Type.INITIAL;

        int dataSize = initialHeader.meta(); // which for initial is the size of the record

        ByteBuffer result = allocator.allocate(dataSize);

        int remaining = dataSize;
        int metaSize = Type.metaSize();
        int frameAvailable = frameSize - metaSize;
        int toRead = Math.min(frameAvailable, remaining );
        frame.limit( metaSize + toRead );
        result.put(frame);

        remaining -= toRead;

        while( remaining > 0 ) {
            frameIndex ++;
            frame = dfb.get( frameIndex );
            RecordHeader header = RecordHeader.fromBuffer(frame);
            if( header.id() != frameIndex ) {
                throw new IllegalStateException("Corrupt datafile, was expecting id=" + frameIndex + " but got:" + header);
            }
            toRead = Math.min(frameAvailable, remaining );
            frame.position(metaSize).limit(metaSize + toRead);
            result.put( frame );
        }
        result.flip();

        return result;
    }

    private int averageFrameCount() {
        return (int) (frameCount / recordCount) ; // todo make averageFrameCount a lot smarter
    }


    RecordHeader readHeader( long index ) throws IOException {
        return RecordHeader.fromBuffer(dfb.get(index));
    }

    ByteBuffer readFrame( long index ) throws IOException {
        return dfb.get(index);
    }

    @Override
    public void close() throws Exception {
        dfb.close();
    }
}
