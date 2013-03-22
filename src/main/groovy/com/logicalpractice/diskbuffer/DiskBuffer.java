package com.logicalpractice.diskbuffer;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.OpenOption;
import java.nio.file.StandardOpenOption;

import static java.nio.file.StandardOpenOption.READ;
import static java.nio.file.StandardOpenOption.WRITE;

/**
 *
 */
public class DiskBuffer implements AutoCloseable {

    enum Type {
        INITIAL(4),
        CONTINUATION(0);

        private final int metaSize;

        Type(int additional) {
            metaSize = 8 + 1 + additional;
        }
        public int metaSize(){ return metaSize ; }
    }

    public static DiskBuffer open( File file ) throws IOException {
        FileChannel fileChannel = FileChannel.open(file.toPath(), READ, WRITE);
        return new DiskBuffer( fileChannel, 0, 2048);
    }

    private final long start ;
    private final int frameSize ;

    private long end = 0;

    private FileChannel fileChannel;

    private ByteBuffer frameBuffer ;

    private DiskBuffer( FileChannel fileChannel, long start, int frameSize) {
        this.start = start;
        this.frameSize = frameSize;
        this.fileChannel = fileChannel;
        this.frameBuffer = ByteBuffer.allocateDirect(frameSize);
    }

    public long start(){ return start ;}
    public long end(){ return end;}
    public long size(){ return end - start;}

    /**
     * Append the readable contains of the given byte buffer
     * @param buffer required buffer
     * @return record id
     */
    public long append( ByteBuffer buffer ) throws IOException {
        int recordSize = buffer.limit() - buffer.position();

        long recordId = allocateId();
        int written = 0;
        while( written < recordSize) {
            Type type = written == 0 ? Type.INITIAL : Type.CONTINUATION ;
            int remaining = recordSize - written;
            int availableFrame = frameSize - type.metaSize();
            int toWrite = Math.min(remaining, availableFrame );
            frameBuffer.clear();
            frameBuffer.putLong(recordId);

            switch( type ){
                case INITIAL:
                    frameBuffer.put( (byte)type.ordinal() ).putInt( recordSize );
                    break;
                case CONTINUATION:
                    frameBuffer.put( (byte)type.ordinal() );
                    break;
                default:
                    throw new AssertionError("unknown type");
            }

            // advance the limit
            buffer.limit(written + toWrite);

            frameBuffer.put(buffer);

            frameBuffer
                .flip()
                .limit(frameSize);

            int progress = 0;
            while( progress < frameSize ) {
                progress = fileChannel.write(frameBuffer);
            }

            written += toWrite;
        }
        fileChannel.force(false); //sync to disk

        return recordId;
    }

    private long allocateId() {
        end = end + 1;
        return end;
    }

    public ByteBuffer get( long id ){
        throw new UnsupportedOperationException("wibble");
    }

    @Override
    public void close() throws Exception {
        fileChannel.close();
    }
}
