package com.example.testtaskagencyamazon.utils;

import static java.math.RoundingMode.HALF_UP;

import java.math.BigDecimal;
import java.math.MathContext;
import java.util.Objects;

public class MathUtil {

    public static final int SCALE = 2;

    public static final MathContext ROUND = MathContext.DECIMAL128;

    public static final MathContext FINAL_ROUND = new MathContext(4, HALF_UP);

    public static BigDecimal finalRound(BigDecimal value) {
        return value.setScale(SCALE, HALF_UP);
    }

    public static Double finalRound(Double value) {
        return new BigDecimal(value).setScale(SCALE, HALF_UP).doubleValue();
    }

    /**
     * Calculate Click Through Rate
     *
     * @param clicks      number of clicks
     * @param impressions number of impressions
     * @return CTR
     */
    public static BigDecimal calculateCTR(Integer clicks, Integer impressions) {
        return calculatePercentage(new BigDecimal(clicks), new BigDecimal(impressions));
    }

    /**
     * Calculate Advertising Cost of Sales (or Total Advertising Cost of Sales)
     *
     * @param cost  advertising cost
     * @param sales sales
     * @return ACoS (TACoS)
     */
    public static BigDecimal calculateACoS(BigDecimal cost, BigDecimal sales) {
        return calculatePercentage(cost, sales);
    }

    /**
     * Calculate Cost Per Click
     *
     * @param cost   advertising cost
     * @param clicks number of clicks
     * @return CPC
     */
    public static BigDecimal calculateCPC(BigDecimal cost, Integer clicks) {
        Objects.requireNonNull(cost);
        Objects.requireNonNull(clicks);

        if (clicks == 0) {
            return BigDecimal.ZERO;
        }
        return cost
                .divide(new BigDecimal(clicks), ROUND);
    }

    /**
     * Calculate Conversion Rate
     *
     * @param purchases number of purchases
     * @param clicks    number of clicks
     * @return Conversion Rate
     */
    public static BigDecimal calculateConversion(Integer purchases, Integer clicks) {
        return calculatePercentage(new BigDecimal(purchases), new BigDecimal(clicks));
    }

    /**
     * Calculate percentage of value from total
     *
     * @param value value
     * @param total total
     * @return percentage
     */
    public static BigDecimal calculatePercentage(BigDecimal value, BigDecimal total) {
        Objects.requireNonNull(value);
        Objects.requireNonNull(total);

        if (total.compareTo(BigDecimal.ZERO) == 0) {
            return BigDecimal.ZERO;
        }
        return value
                .divide(total, ROUND)
                .multiply(new BigDecimal(100));
    }
}
