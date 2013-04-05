package com.logicalpractice.diskbuffer

import spock.lang.Specification

import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets

/**
 *
 */
class DiskBufferSpecification extends Specification {

    File file = File.createTempFile("diskbuffer", "dat")

    DiskBuffer testObject = DiskBuffer.newBuilder().withPath(file.toPath()).build()

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

    def "readHeader returns id and type for index 0"(){
        setup:
        testObject.append( ByteBuffer.wrap(bytes4000) )

        when:
        def result = testObject.readHeader(0)

        then:
        result.id() == 1L
        result.type() == DiskBuffer.Type.INITIAL
    }

    def "readHeader returns id and type CONTIN for index 1"(){
        setup:
        testObject.append( ByteBuffer.wrap(bytes4000) )
        when:
        def result = testObject.readHeader(1)

        then:
        result.id() == 1L
        result.type() == DiskBuffer.Type.CONTINUATION
    }

    def "readFrame returns buffer of limit frameSize for index 0"(){
        setup:
        testObject.append( ByteBuffer.wrap(bytes4000) )

        when:
        def result = testObject.readFrame( 0 )

        then:
        result.limit() == 2048
        result.getLong() == 1L
    }

    def "readFrame returns buffer of limit frameSize for index 1"(){
        setup:
        testObject.append( ByteBuffer.wrap(bytes4000) )

        when:
        def result = testObject.readFrame( 1 )

        then:
        result.limit() == 2048
        result.getLong() == 1L
    }

    def "get() throws IllegalArgumentException if id is less than start"(){
        when:
        testObject.get( 0L )

        then:
        thrown(IllegalArgumentException)
    }

    def "get() throws IllegalArgumentException if id is greater than end"(){
        setup:
        testObject.append( ByteBuffer.wrap(bytes4000) )

        when:
        testObject.get(2L)

        then:
        thrown(IllegalArgumentException)
    }

    def "single dataFrame single record get(end) returns buffer with contents"(){
        def input = "Hello World".bytes

        setup:
        testObject.append( ByteBuffer.wrap(input) )

        when:
        def result = testObject.get( 1L )
        def out = ByteArrays.allocate(result.remaining())
        result.get(out)
        def str = new String(out)

        then:
        result.limit() == input.length
        str == "Hello World"
    }

    def "3 single dataFrame Record get(end) returns buffer with contents"(){
        setup:
        3.times {testObject.append( ByteBuffer.wrap("Hello World ${it + 1}".bytes) )}

        when:
        def result = testObject.get( 3L )
        def out = ByteArrays.allocate(result.remaining())
        result.get(out)
        def str = new String(out)

        then:
        result.limit() == "Hello World 3".length()
        str == "Hello World 3"
    }

    def "3 single dataFrame Record get(1) returns buffer with contents"(){
        setup:
        3.times {testObject.append( ByteBuffer.wrap("Hello World ${it + 1}".bytes) )}

        when:
        def result = testObject.get( 1L )
        def out = ByteArrays.allocate(result.remaining())
        result.get(out)
        def str = new String(out)

        then:
        result.limit() == "Hello World 1".length()
        str == "Hello World 1"
    }

    def "3 single dataFrame Record get(2) returns buffer with contents"(){
        setup:
        3.times {testObject.append( ByteBuffer.wrap("Hello World ${it + 1}".bytes) )}

        when:
        def result = testObject.get( 2L )
        def out = ByteArrays.allocate(result.remaining())
        result.get(out)
        def str = new String(out)

        then:
        result.limit() == "Hello World 2".length()
        str == "Hello World 2"
    }

    def "300 single dataFrame Records select multiple"(long index){
        setup:
        300.times {testObject.append( ByteBuffer.wrap("Hello World ${it + 1}".bytes) )}

        when:

        println "get(${index})"
        def result = testObject.get( index )
        def out = ByteArrays.allocate(result.remaining())
        result.get(out)
        def str = new String(out)

        then:
        result.limit() == "Hello World ${index}".length()
        str == "Hello World ${index}"

        where:
        index << [ 3, 30, 40 , 77, 110, 232, 299]
    }
}
