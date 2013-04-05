package com.logicalpractice.diskbuffer

import spock.lang.Specification

import java.nio.ByteBuffer

/**
 *
 */
class DiskBuffer2Specification extends Specification {

    File file = File.createTempFile("diskbuffer", "dat")

    def "reopen a file"(){
        setup:
        DiskBuffer buffer = DiskBuffer.newBuilder()
                                        .withPath(file.toPath())
                                        .build()
        300.times {buffer.append( ByteBuffer.wrap("Hello World ${it + 1}".bytes) )}
        buffer.close()

        when:
        DiskBuffer testObject = DiskBuffer.newBuilder().withPath(file.toPath()).build()

        then:
        testObject.end() == 300L

        cleanup:
        testObject.close()
        file.delete()
    }

}
