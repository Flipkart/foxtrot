package com.flipkart.foxtrot.common.stats;

public enum Stat {

<<<<<<< HEAD
    COUNT(false) {
=======
    count(false) {
>>>>>>> phonepe-develop
        @Override
        public <T> T visit(StatVisitor<T> visitor) {
            return visitor.visitCount();
        }
    },
<<<<<<< HEAD
    MIN(false) {
=======
    min(false) {
>>>>>>> phonepe-develop
        @Override
        public <T> T visit(StatVisitor<T> visitor) {
            return visitor.visitMin();
        }
    },
<<<<<<< HEAD
    MAX(false) {
=======
    max(false) {
>>>>>>> phonepe-develop
        @Override
        public <T> T visit(StatVisitor<T> visitor) {
            return visitor.visitMax();
        }
    },
<<<<<<< HEAD
    AVG(false) {
=======
    avg(false) {
>>>>>>> phonepe-develop
        @Override
        public <T> T visit(StatVisitor<T> visitor) {
            return visitor.visitAvg();
        }
    },
<<<<<<< HEAD
    SUM(false) {
=======
    sum(false) {
>>>>>>> phonepe-develop
        @Override
        public <T> T visit(StatVisitor<T> visitor) {
            return visitor.visitSum();
        }
    },
<<<<<<< HEAD
    SUM_OF_SQUARES(true) {
=======
    sum_of_squares(true) {
>>>>>>> phonepe-develop
        @Override
        public <T> T visit(StatVisitor<T> visitor) {
            return visitor.visitSumOfSquares();
        }
    },
<<<<<<< HEAD
    VARIANCE(true) {
=======
    variance(true) {
>>>>>>> phonepe-develop
        @Override
        public <T> T visit(StatVisitor<T> visitor) {
            return visitor.visitVariance();
        }
    },
<<<<<<< HEAD
    STD_DEVIATION(true) {
=======
    std_deviation(true) {
>>>>>>> phonepe-develop
        @Override
        public <T> T visit(StatVisitor<T> visitor) {
            return visitor.visitStdDeviation();
        }
    };

<<<<<<< HEAD
    private boolean extended;
=======
    public boolean extended;
>>>>>>> phonepe-develop

    Stat(boolean extended) {
        this.extended = extended;
    }

    public boolean isExtended() {
        return extended;
    }

<<<<<<< HEAD
    public abstract <T> T visit(StatVisitor<T> visitor);
=======
>>>>>>> phonepe-develop

    public interface StatVisitor<T> {

        T visitCount();

        T visitMin();

        T visitMax();

        T visitAvg();

        T visitSum();

        T visitSumOfSquares();

        T visitVariance();

        T visitStdDeviation();
    }
<<<<<<< HEAD
=======

    public abstract <T> T visit(StatVisitor<T> visitor);
>>>>>>> phonepe-develop
}
