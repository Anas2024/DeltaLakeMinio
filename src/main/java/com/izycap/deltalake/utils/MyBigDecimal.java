package com.izycap.deltalake.utils;

import java.math.BigDecimal;
import java.math.MathContext;
import java.math.RoundingMode;

public class MyBigDecimal extends BigDecimal {

    private static final int PRECISION = 10;
    private static final int SCALE = 2;

    public MyBigDecimal(String val) {
        super(val, new MathContext(PRECISION));
        setScale(SCALE, RoundingMode.HALF_UP);
    }

    public MyBigDecimal(double val) {
        this(String.valueOf(val));
    }

    public MyBigDecimal(BigDecimal val) {
        this(val.toString());
    }


}
