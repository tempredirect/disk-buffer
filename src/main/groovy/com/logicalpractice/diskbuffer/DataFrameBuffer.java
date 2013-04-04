package com.logicalpractice.diskbuffer;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.List;

/**
 *
 */
public class DataFrameBuffer {

    private int pageSize = (int)Math.pow(2, 16); // 64k

    private int frameSize = pageSize / 32; // 2K

    private long pageCount = 0 ;
    private long frameCount = 0;

    private ByteBuffer lastPage ;

    private final FileChannel fileChannel;

    private final BufferAllocator allocator = new DirectBufferAllocator();

    public DataFrameBuffer(FileChannel fileChannel) {
        this.fileChannel = fileChannel;
        this.lastPage = allocator.allocate(pageSize);
        // todo read the last page into the 'lastPage' buffer
    }

    public void append(ByteBuffer frame) throws IOException {
        append(new ByteBuffer[]{frame} );
    }
    public void append(ByteBuffer [] frames) throws IOException {
        checkBuffersSizes(frames);
        int framesWritten = 0;
        while( framesWritten < frames.length ) {

            int remainingFrames = ensureRemainingFrames();
            assert remainingFrames > 0: "Should have more than 0 remaining frames";
            int framesToWrite = Math.min(remainingFrames,frames.length - framesWritten);

            ByteBuffer [] toWrite = new ByteBuffer[framesToWrite];
            System.arraycopy(frames, framesWritten, toWrite, 0, framesToWrite);

            int expected = framesToWrite * frameSize;
            int written = 0;

            while( written < expected ) written += fileChannel.write(toWrite);

            for (ByteBuffer frame : toWrite) {
                frame.flip();
                lastPage.put(frame);
            }
            framesWritten += framesToWrite;
        }

        fileChannel.force(false);
        frameCount += framesWritten;
    }

    private void checkBuffersSizes(ByteBuffer[] frames) {
        for (ByteBuffer frame : frames) {
            Preconditions.checkArgument(frame.remaining() == frameSize, "frame must be of exactly frameSize");
        }
    }

    private int ensureRemainingFrames() {
        int remaining = lastPage.remaining() / frameSize;
        if( remaining == 0 ) {
            ByteBuffer toRecycle = lastPage;
            lastPage = allocator.allocate(pageSize);
            pageCount ++;
            allocator.recycle(toRecycle);
            remaining = pageSize / frameSize;
        }
        return remaining;
    }

    public ByteBuffer get( long index ) throws IOException {
        if( index >= frameCount ) {
            throw new IllegalArgumentException("index is greater than current last frame");
        }
        if( index < 0 ) {
            throw new IllegalArgumentException("index is less than zero");
        }
        long page = index / pageSize;
        int offset = (int)(index % pageSize) * frameSize;
        ByteBuffer pageBuffer = page(page);

        ByteBuffer result = pageBuffer.duplicate();

        result.limit(offset + frameSize)
              .position(offset);

        return result.asReadOnlyBuffer();
    }

    private ByteBuffer page( long pageNumber ) throws IOException {

        if( pageNumber == pageCount ) {
            // is last page
            return lastPage;
        }
        // todo cache pages in an LRU list
        return loadPage( pageNumber );
    }

    private ByteBuffer loadPage( long pageNumber ) throws IOException {
        ByteBuffer page = allocator.allocate(pageSize);

        int toRead = pageSize;
        long offset = pageNumber * pageSize;
        int haveRead = 0;
        while( haveRead < toRead ) {
            haveRead += fileChannel.read(page, offset + haveRead);
        }

        return page;
    }

    public int getFrameSize() { return frameSize; }
}
