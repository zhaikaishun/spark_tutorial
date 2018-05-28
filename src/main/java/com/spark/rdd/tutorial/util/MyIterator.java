package com.spark.rdd.tutorial.util;

import java.util.Iterator;

public class MyIterator<T> implements Iterator, Iterable
{
    private Iterator myIterable;

    public MyIterator(Iterable iterable)
    {
        myIterable = iterable.iterator();
    }

    @Override
    public boolean hasNext()
    {
        return myIterable.hasNext();
    }

    @Override
    public Object next()
    {
        return myIterable.next();
    }

    @Override
    public void remove()
    {
        myIterable.remove();
    }

    @Override
    public Iterator iterator()
    {
        return myIterable;
    }
}
