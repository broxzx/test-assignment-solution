package com.example.testtaskagencyamazon.utils;

import jakarta.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;
import org.springframework.stereotype.Component;

@Component
public class ObjectsUtil {

    /**
     * Merge two lists into one
     *
     * @param list1 the first list
     * @param list2 the second list
     * @param <T>   the type of the list
     *
     * @return the merged list
     */
    public static <T> List<T> mergeLists(List<T> list1, List<T> list2) {
        if (list1 == null) {
            return list2;
        }

        if (list2 == null) {
            return list1;
        }

        List<T> mergedList = new ArrayList<>(list1);
        mergedList.addAll(list2);

        return mergedList;
    }

    /**
     * Parse a string to a Long
     *
     * @param value the value to parse
     *
     * @return the parsed Long or null if the value is null or not a number
     */
    public static Long parseLong(@Nullable String value) {
        if (value == null) {
            return null;
        }

        try {
            return Long.parseLong(value);
        } catch (NumberFormatException e) {
            return null;
        }
    }

    /**
     * Check if the condition is met, if not throw an IllegalArgumentException with the given message
     *
     * @param condition the condition to check
     * @param message   the message to throw
     *
     * @throws IllegalArgumentException if the condition is not met
     */
    public static void checkAndThrow(boolean condition, String message) {
        if (condition) {
            throw new IllegalArgumentException(message);
        }
    }

}
