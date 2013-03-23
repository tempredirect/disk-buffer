package com.logicalpractice.diskbuffer;

import java.nio.ByteBuffer;

/**
 *
 */
public class DirectBufferAllocator implements BufferAllocator {

    @Override
    public ByteBuffer allocate(int capacity) {
        return ByteBuffer.allocateDirect(capacity);
    }

    @Override
    public void recycle(ByteBuffer buffer) {
        // is a NOP
    }
}
