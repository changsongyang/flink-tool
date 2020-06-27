package com.lzhpo.flinkmysql.source;

import java.io.Serializable;

/**
 * @author lzhpo
 */
public class Student implements Serializable {
    private Long id;
    private String name;
    private String course;
    private Double score;

    public Student() {
    }

    public Student(Long id, String name, String course, Double score) {
        this.id = id;
        this.name = name;
        this.course = course;
        this.score = score;
    }

    @Override
    public String toString() {
        return "Student{" +
                "id=" + id +
                ", name='" + name + '\'' +
                ", course='" + course + '\'' +
                ", score=" + score +
                '}';
    }

    /**
     * builder
     *
     * @return Builder
     */
    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {
        private final Student student;

        public Builder() {
            student = new Student();
        }

        public Builder id(Long id) {
            student.id = id;
            return this;
        }

        public Builder name(String name) {
            student.name = name;
            return this;
        }

        public Builder course(String course) {
            student.course = course;
            return this;
        }

        public Builder score(Double score) {
            student.score = score;
            return this;
        }

        public Student builded() {
            return student;
        }
    }
}
