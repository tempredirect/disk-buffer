package com.logicalpractice.diskbuffer

import org.junit.After
import org.junit.AfterClass
import org.junit.Before
import org.junit.BeforeClass
import org.junit.Test

import java.nio.ByteBuffer

/**
 *
 */
class DiskBufferPerformanceTest {

    static File file = File.createTempFile("diskbuffer", "dat")

    static DataFrameBuffer dfb = DataFrameBuffer.open(file.toPath())

    static DiskBuffer testObject = DiskBuffer.newWith(dfb)

    @BeforeClass
    public static void setup(){
        300.times { testObject.append( ByteBuffer.wrap("Hello World ${it + 1}".bytes) ) }
    }

    @AfterClass
    public static void tearDown(){
        testObject.close()
    }

    @Test
    public void "single record access"(){
        get(77)
     }

    @Test
    public void "multiple record access"(){
        [ 3, 30, 40 , 77, 110, 232, 299].each {
            get(it)
        }
    }

    @Test
    public void "seq read through all records"(){
        (1..300).each {
            get(it)
        }
    }

    def get(index){
        println "get(${index})"
        def result = testObject.get( index )
        def out = ByteArrays.allocate(result.remaining())
        result.get(out)
        def str = new String(out)

        assert result.limit() == "Hello World ${index}".length()
        assert str == "Hello World ${index}"
    }

    @Before
    public void reset(){
        dfb.readStat.reset()
        dfb.writeStat.reset()
    }

    @After
    public void report(){
        println( "ReadStat ${dfb.readStat}")
    }
}
