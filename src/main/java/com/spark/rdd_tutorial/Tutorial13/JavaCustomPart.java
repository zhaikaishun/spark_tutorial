package com.spark.rdd_tutorial.Tutorial13;

import org.apache.spark.Partitioner;

/**
 * 自定义分区器
 * Created by zhaikaishun on 2017/8/20.
 */
public class JavaCustomPart extends Partitioner {
    int i = 1;
    public JavaCustomPart(int i){
        this.i=i;
    }
    public JavaCustomPart(){}
    @Override
    public int numPartitions() {
        return i;
    }

    @Override
    public int getPartition(Object key) {
        int keyCode = Integer.parseInt(key.toString());
        if(keyCode>=4){
            return 0;
        }else if(keyCode>=2&&keyCode<4){
            return 1;
        }else {
            return 2;
        }
    }
}
