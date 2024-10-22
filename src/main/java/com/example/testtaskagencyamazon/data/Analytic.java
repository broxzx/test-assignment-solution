package com.example.testtaskagencyamazon.data;

public interface Analytic<T> {

    T add(T other);

    void finalise();

}
