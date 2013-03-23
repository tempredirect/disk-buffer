package com.logicalpractice.diskbuffer;

import java.nio.ByteBuffer;

/**
 *
 */
public interface BufferAllocator {

    ByteBuffer allocate( int size );

    void recycle( ByteBuffer buffer );
}
