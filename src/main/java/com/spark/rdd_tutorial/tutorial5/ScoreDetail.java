package com.spark.rdd_tutorial.tutorial5;

import java.io.Serializable;

/**
 * Created by zhaikaishun on 2017/8/20.
 */
public class ScoreDetail implements Serializable {
    //case class ScoreDetail(studentName: String, subject: String, score: Float)
    public String studentName;
    public String subject;
    public float score;

    public ScoreDetail(String studentName, String subject, float score) {
        this.studentName = studentName;
        this.subject = subject;
        this.score = score;
    }
}
