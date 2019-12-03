package com.globalegrow.presto;

import com.facebook.presto.spi.Plugin;
import com.google.common.collect.ImmutableSet;

import java.util.Set;

/**
 * @author 小和尚
 * @version 1.0.0
 * @ClassName ProstoPlugin.UDF_Plugin
 * @Description TODO
 * @createTime 2019年11月29日 14:47:00
 */
public class UDF_Plugin implements Plugin {
    @Override
    public Set<Class<?>> getFunctions(){

        return ImmutableSet.<Class<?>>builder()
                .add(BitMapAggregationFunction.class)
                .build();
    }

}