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

    DataFrameBuffer testObject

    def cleanup(){
        if(testObject != null)
            testObject.close()
    }

    def "Append frameSize"() {
        setup:
        testObject = DataFrameBuffer.open(file.toPath())
        when:
        ByteBuffer buffer = ByteBuffer.allocate(testObject.frameSize)
        testObject.append( [buffer] as ByteBuffer[] )

        then:
        file.length() == testObject.frameSize
    }

    def "Append 5 frames"() {
        setup:
        testObject = DataFrameBuffer.open(file.toPath())

        when:
        def frames = (1..5).collect{ByteBuffer.allocate(testObject.frameSize)}
        testObject.append( frames as ByteBuffer[])

        then:
        file.length() == testObject.frameSize * frames.size()
    }

    def "Append 50 frames"(){
        setup:
        testObject = DataFrameBuffer.open(file.toPath())

        when:
        def frames = (1..50).collect{ index ->
            def b = ByteBuffer.allocate(testObject.frameSize)
            b.put("${index}".bytes)
            b.clear()
            b
        }
        testObject.append( frames as ByteBuffer[] )

        then:
        file.length() == testObject.frameSize * 10 * 5
    }
    def "Append 5 frames 10 times"(){
        setup:
        testObject = DataFrameBuffer.open(file.toPath())

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
        file.length() == testObject.frameSize * 10 * 5
    }

    def "Append & get 1 frame"() {
        setup:
        testObject = DataFrameBuffer.open(file.toPath())

        when:
        def frame = frameWith("Value")
        testObject.append(frame)
        def result = testObject.get(0L)

        then:
        result != null
        result.position() == 0
        result.limit() == testObject.frameSize
        checkBuffer(result, "Value")
    }

    def "Append more than a single page & get 1 frame"() {
        setup:
        testObject = DataFrameBuffer.open(file.toPath())

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
        checkBuffer(result, "Value 0")
    }

    def "get() on empty buffer"() {
        setup:
        testObject = DataFrameBuffer.open(file.toPath())

        when:
        testObject.get(0L)

        then:
        thrown(IllegalArgumentException)
    }

    def "get(-1) on empty buffer"() {
        setup:
        testObject = DataFrameBuffer.open(file.toPath())

        when:
        testObject.get(-1L)

        then:
        thrown(IllegalArgumentException)
    }

    def "get(10) on 9 long buffer"() {
        setup:
        testObject = DataFrameBuffer.open(file.toPath())

        when:
        9.times {
            testObject.append(frameWith("wibble"))
        }
        testObject.get(10L)

        then:
        thrown(IllegalArgumentException)
    }


    def "open a file that has a single Page"(){
        setup:
        FileChannel fc = null
        try{
            fc = FileChannel.open(file.toPath(),StandardOpenOption.WRITE)
            ByteBuffer buff = ByteBuffer.allocate(DataFrameBuffer.DEFAULT_PAGE_SIZE)
            int written = 0
            while( written < DataFrameBuffer.DEFAULT_PAGE_SIZE)
                written += fc.write(buff)
            fc.force(false)
        }finally{
            fc.close()
        }

        when:
        testObject = DataFrameBuffer.open(file.toPath())

        then:
        testObject.frameCount == DataFrameBuffer.DEFAULT_PAGE_SIZE / DataFrameBuffer.DEFAULT_FRAME_SIZE
    }

    def "open a file that has a single complete page and a partial page with 5 frames"(){
        setup:
        FileChannel fc = null
        try{
            fc = FileChannel.open(file.toPath(),StandardOpenOption.WRITE)
            ByteBuffer buff = ByteBuffer.allocate(DataFrameBuffer.DEFAULT_PAGE_SIZE)
            int written = 0
            while( written < DataFrameBuffer.DEFAULT_PAGE_SIZE)
                written += fc.write(buff)

            buff.clear()
            buff.limit(DataFrameBuffer.DEFAULT_FRAME_SIZE * 5)

            written = 0
            while( written < DataFrameBuffer.DEFAULT_FRAME_SIZE * 5)
                written += fc.write(buff)

            fc.force(false)

        }finally{
            fc.close()
        }

        when:
        testObject = DataFrameBuffer.open(file.toPath())

        then:
        testObject.frameCount == (DataFrameBuffer.DEFAULT_PAGE_SIZE / DataFrameBuffer.DEFAULT_FRAME_SIZE) + 5
    }

    def frameWith(String content) {
        def b = ByteBuffer.allocate(testObject.frameSize)
        b.put(content.bytes)
        b.clear()
        b
    }

    def checkBuffer( ByteBuffer buff, String value){
        value.each { c ->
            assert buff.get() == c.charAt(0)
        }
    }
}
