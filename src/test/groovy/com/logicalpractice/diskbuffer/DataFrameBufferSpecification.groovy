package com.logicalpractice.diskbuffer

import spock.lang.Specification

import java.nio.ByteBuffer
import java.nio.channels.FileChannel
import java.nio.file.StandardOpenOption

/**
 *
 */
class DataFrameBufferSpecification extends Specification {

    File file = File.createTempFile("dataFrameBuffer", "dat")

    FileChannel fileChannel = FileChannel.open(file.toPath(), StandardOpenOption.READ, StandardOpenOption.WRITE)

    DataFrameBuffer testObject = new DataFrameBuffer(fileChannel)

    def cleanup(){
        fileChannel.close()
    }

    def "Append frameSize"() {
        when:
        ByteBuffer buffer = ByteBuffer.allocate(testObject.frameSize)
        testObject.append( [buffer] as ByteBuffer[] )

        then:
        fileChannel.size() == testObject.frameSize
    }

    def "Append 5 frames"() {
        when:
        def frames = (1..5).collect{ByteBuffer.allocate(testObject.frameSize)}
        testObject.append( frames as ByteBuffer[])

        then:
        fileChannel.size() == testObject.frameSize * frames.size()
    }

    def "Append 50 frames"(){
        when:
        def frames = (1..50).collect{ index ->
            def b = ByteBuffer.allocate(testObject.frameSize)
            b.put("${index}".bytes)
            b.clear()
            b
        }
        testObject.append( frames as ByteBuffer[] )

        then:
        fileChannel.size() == testObject.frameSize * 10 * 5
    }
    def "Append 5 frames 10 times"(){
        when:
        10.times { loop ->
            def frames = (1..5).collect{ index ->
                def b = ByteBuffer.allocate(testObject.frameSize)
                b.put("${index * loop}".bytes)
                b.clear()
                b
            }
            testObject.append( frames as ByteBuffer[] )
        }

        then:
        fileChannel.size() == testObject.frameSize * 10 * 5
    }

    def "Append & get 1 frame"() {
        when:
        def frame = frameWith("Value")
        testObject.append(frame)
        def result = testObject.get(0L)

        then:
        result != null
        result.position() == 0
        result.limit() == testObject.frameSize
        result.get() == 'V' as char
        result.get() == 'a' as char
        result.get() == 'l' as char
        result.get() == 'u' as char
        result.get() == 'e' as char
    }

    def "Append more than a single page & get 1 frame"() {
        when:
        40.times {
            def frame = frameWith("Value ${0}")
            testObject.append(frame)
        }
        def result = testObject.get(0L)

        then:
        result != null
        result.position() == 0
        result.limit() == testObject.frameSize
        result.get() == 'V' as char
        result.get() == 'a' as char
        result.get() == 'l' as char
        result.get() == 'u' as char
        result.get() == 'e' as char
        result.get() == ' ' as char
        result.get() == '0' as char
    }

    def "get() on empty buffer"() {
        when:
        testObject.get(0L)

        then:
        thrown(IllegalArgumentException)
    }

    def "get(-1) on empty buffer"() {
        when:
        testObject.get(-1L)

        then:
        thrown(IllegalArgumentException)
    }

    def "get(10) on 9 long buffer"() {
        when:
        9.times {
            testObject.append(frameWith("wibble"))
        }
        testObject.get(10L)

        then:
        thrown(IllegalArgumentException)
    }

    def frameWith(String content) {
        def b = ByteBuffer.allocate(testObject.frameSize)
        b.put(content.bytes)
        b.clear()
        b
    }
}
