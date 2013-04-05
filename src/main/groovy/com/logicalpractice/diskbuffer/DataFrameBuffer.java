package com.logicalpractice.diskbuffer;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;

/**
 *
 */
public final class DataFrameBuffer implements AutoCloseable {

    public static final int DEFAULT_PAGE_SIZE = (int)Math.pow(2, 16); // 64k

    public static final int DEFAULT_FRAME_SIZE = DEFAULT_PAGE_SIZE / 32; // 2k

    public static DataFrameBuffer open(Path path, BufferAllocator allocator, int pageSize, int frameSize)
            throws IOException {

        checkArgument( path != null, "'path' is required");
        checkArgument( allocator != null, "'allocator' is required");
        checkArgument( pageSize > 0, "'pageSize' must be positive");
        checkArgument( frameSize > 0, "'frameSize' must be positive");
        checkArgument( pageSize % frameSize == 0, "'frameSize' must be a factor of 'pageSize'");

        FileChannel fc = FileChannel.open(path, StandardOpenOption.READ, StandardOpenOption.WRITE, StandardOpenOption.CREATE);

        DataFrameBuffer dfb = new DataFrameBuffer(fc, allocator, pageSize, frameSize);

        dfb.initialise(); // will load the lastPage and initialise the internals

        return dfb;
    }

    public static DataFrameBuffer open(Path path) throws IOException {
        return open( path, new DirectBufferAllocator(), DataFrameBuffer.DEFAULT_PAGE_SIZE, DataFrameBuffer.DEFAULT_FRAME_SIZE);
    }

    private final int pageSize;

    private final int frameSize ;

    private long pageCount = 0 ;  // complete pages
    private long frameCount = 0;

    private ByteBuffer lastPage ;

    private final FileChannel fileChannel;

    private final BufferAllocator allocator;

    private DataFrameBuffer(FileChannel fileChannel, BufferAllocator allocator, int pageSize, int frameSize) {
        this.fileChannel = fileChannel;
        this.allocator = allocator;
        this.pageSize = pageSize;
        this.frameSize = frameSize;
    }

    private void initialise() throws IOException {
        lastPage = allocator.allocate(pageSize);
        long size = fileChannel.size();
        if( size == 0 ){
            pageCount = 0;
            frameCount = 0;
        } else {
            if( size % frameSize > 0 ) {
                throw new IllegalStateException("Unable to handle corrupt frame files at the moment");
            }
            long completePages = size / pageSize ;
            long lastPageOffset = completePages * pageSize;
            long lastPageSize = size - lastPageOffset;
            if( lastPageSize > 0 ){
                int read = 0;
                while( read < lastPageSize ){
                    read += fileChannel.read(lastPage, lastPageOffset + read);
                }
            }
            pageCount = completePages;
            frameCount = completePages * (pageSize / frameSize) + (lastPageSize / frameSize) ;
        }
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
            checkArgument(frame.remaining() == frameSize, "frame must be of exactly frameSize");
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

    public long size() throws IOException { return fileChannel.size(); }

    public long getFrameCount() { return frameCount; }

    @Override
    public void close() throws Exception {
        fileChannel.close();
    }
}
