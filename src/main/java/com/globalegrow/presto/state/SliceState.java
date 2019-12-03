package com.globalegrow.presto.state;

/**
 * @author 小和尚
 * @version 1.0.0
 * @ClassName ProstoPlugin.SliceState
 * @Description TODO
 * @createTime 2019年11月29日 18:07:00
 */
import com.facebook.presto.spi.function.AccumulatorState;
import io.airlift.slice.Slice;

public interface SliceState extends AccumulatorState{

    void setSlice(Slice value);

    Slice getSlice();

}