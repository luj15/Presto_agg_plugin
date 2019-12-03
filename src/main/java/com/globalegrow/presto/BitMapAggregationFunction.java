package com.globalegrow.presto;

import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.function.*;
import com.facebook.presto.spi.type.StandardTypes;
import com.globalegrow.presto.state.SliceState;
import com.globalegrow.presto.utils.BitMapSerOrDeser;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import org.roaringbitmap.RoaringBitmap;

import java.io.IOException;

import static com.facebook.presto.spi.type.BigintType.BIGINT;

/**
 * @author 小和尚
 * @version 1.0.0
 * @ClassName ProstoPlugin.com.globalegrow.presto.BitMapAggregationFunction
 * @Description TODO
 * @createTime 2019年11月29日 12:18:00
 */
@AggregationFunction("count_distinct_bitmap")
@Description("BitMap并集结果")
public final class BitMapAggregationFunction {
    @InputFunction
    public static void input(@AggregationState SliceState state, @SqlType(StandardTypes.VARCHAR) Slice input) {
        // 这里实现计算逻辑，得到 updatedValue

        if (state.getSlice() == null) {

            String bitMap_str_input = input.toStringUtf8();

            RoaringBitmap bitMap_input = null;
            try {
                bitMap_input = BitMapSerOrDeser.getBitMapObject(bitMap_str_input);
            } catch (IOException e) {
                bitMap_input = new RoaringBitmap();
            }

            String serialize = BitMapSerOrDeser.serialize(bitMap_input);

            state.setSlice(Slices.utf8Slice(serialize));

        } else {
            String bitMap_str_state = state.getSlice().toStringUtf8();
            RoaringBitmap bitMap_state = null;
            try {
                bitMap_state = BitMapSerOrDeser.getBitMapObject(bitMap_str_state);
            } catch (IOException e) {
                bitMap_state = new RoaringBitmap();
            }

            String bitMap_str_input = input.toStringUtf8();

            RoaringBitmap bitMap_input = null;
            try {
                bitMap_input = BitMapSerOrDeser.getBitMapObject(bitMap_str_input);
            } catch (IOException e) {
                bitMap_input = new RoaringBitmap();
            }

            bitMap_state.or(bitMap_input);
            String serialize = BitMapSerOrDeser.serialize(bitMap_state);

            state.setSlice(Slices.utf8Slice(serialize));
        }
    }

    @CombineFunction
    public static void combine(@AggregationState SliceState state, @AggregationState SliceState otherState) {
        // 这里实现 merge 逻辑，将合并后的结果 updatedValue 写回到 state 中

        if (state.getSlice() == null && otherState.getSlice() != null) {

            String bitMap_str_one = otherState.getSlice().toStringUtf8();
            RoaringBitmap bitMap_one = null;
            try {
                bitMap_one = BitMapSerOrDeser.getBitMapObject(bitMap_str_one);
            } catch (IOException e) {
                bitMap_one = new RoaringBitmap();
            }
            String serialize = BitMapSerOrDeser.serialize(bitMap_one);
            state.setSlice(Slices.utf8Slice(serialize));

        } else if (state.getSlice() != null && otherState.getSlice() == null) {

            String bitMap_str_one = state.getSlice().toStringUtf8();
            RoaringBitmap bitMap_one = null;
            try {
                bitMap_one = BitMapSerOrDeser.getBitMapObject(bitMap_str_one);
            } catch (IOException e) {
                bitMap_one = new RoaringBitmap();
            }
            String serialize = BitMapSerOrDeser.serialize(bitMap_one);
            state.setSlice(Slices.utf8Slice(serialize));
        } else {

            String bitMap_str_one = state.getSlice().toStringUtf8();
            String bitMap_str_other = otherState.getSlice().toStringUtf8();
            RoaringBitmap bitMap_one = null;
            try {
                bitMap_one = BitMapSerOrDeser.getBitMapObject(bitMap_str_one);
            } catch (IOException e) {
                bitMap_one = new RoaringBitmap();
            }
            RoaringBitmap bitMap_other = null;
            try {
                bitMap_other = BitMapSerOrDeser.getBitMapObject(bitMap_str_other);
            } catch (IOException e) {
                bitMap_other = new RoaringBitmap();
            }
            bitMap_one.or(bitMap_other);
            String serialize = BitMapSerOrDeser.serialize(bitMap_one);
            state.setSlice(Slices.utf8Slice(serialize));

        }


    }

    @OutputFunction(StandardTypes.BIGINT)
    public static void output(@AggregationState SliceState state, BlockBuilder out) throws IOException {
        // 这里将最终结果序列化后写入 blockBuilder，最终写入到 out
        long number = BitMapSerOrDeser.getBitMapObject(state.getSlice().toStringUtf8()).getLongCardinality();
        BIGINT.writeLong(out, number);
        out.closeEntry();
    }

}