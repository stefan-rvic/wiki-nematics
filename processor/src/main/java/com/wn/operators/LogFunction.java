package com.wn.operators;


import lombok.NoArgsConstructor;
import lombok.Setter;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.util.function.SerializableFunction;

import java.util.logging.Level;
import java.util.logging.Logger;

@NoArgsConstructor
@Setter
public class LogFunction<T> implements MapFunction<T, T> {

    private static final long serialVersionUID = 1L;
    private static final Logger LOG = Logger.getLogger(LogFunction.class.getName());

    private SerializableFunction<T, String> formatter = Object::toString;

    private Level level = Level.INFO;

    @Override
    public T map(T t) {
        LOG.log(level, formatter.apply(t));
        return t;
    }
}

