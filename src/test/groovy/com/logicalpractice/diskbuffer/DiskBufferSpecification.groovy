package com.logicalpractice.diskbuffer

import spock.lang.Specification

import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets

/**
 *
 */
class DiskBufferSpecification extends Specification {

    File file = File.createTempFile("diskbuffer", "dat")

    DiskBuffer testObject = DiskBuffer.open(file)

    def bytes4000 = ('a' * 4000).bytes

    def cleanup(){
        testObject.close()
    }

    def "created with start 0 and 0 size"() {
        expect:
        testObject.size() == 0L
        testObject.start() == 0L
    }

    def "appending a single record results in id == 1"(){
        setup:
        def buffer = ByteBuffer.wrap("The quick brown fox".getBytes(StandardCharsets.UTF_8))

        expect:
        testObject.append( buffer ) == 1L
        testObject.end() == 1L
        testObject.size() == 1L
        file.size() == 2048L
    }

    def "id should increase"(){
        when:
        def recordIds = []
        10.times {
            def buffer = ByteBuffer.wrap("The quick brown fox".getBytes(StandardCharsets.UTF_8))
            recordIds << testObject.append( buffer )
        }

        then:
        testObject.end() == 10L
        testObject.size() == 10L
        file.size() == 2048L * 10
        recordIds.inject(0L) { last, current ->
            assert last < current
            current
        }
    }

    def "record over the frame size is stored"(){
        when:
        def id = testObject.append( ByteBuffer.wrap(bytes4000) )

        then:
        id == 1L
        testObject.size() == 1L
        file.size() == 2048L * 2
    }
}
