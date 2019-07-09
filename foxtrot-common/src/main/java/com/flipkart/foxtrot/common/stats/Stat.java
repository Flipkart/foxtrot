package com.flipkart.foxtrot.common.stats;

public enum Stat {

    count(false) {
        @Override
        public <T> T visit(StatVisitor<T> visitor) {
            return visitor.visitCount();
        }
    },
    min(false) {
        @Override
        public <T> T visit(StatVisitor<T> visitor) {
            return visitor.visitMin();
        }
    },
    max(false) {
        @Override
        public <T> T visit(StatVisitor<T> visitor) {
            return visitor.visitMax();
        }
    },
    avg(false) {
        @Override
        public <T> T visit(StatVisitor<T> visitor) {
            return visitor.visitAvg();
        }
    },
    sum(false) {
        @Override
        public <T> T visit(StatVisitor<T> visitor) {
            return visitor.visitSum();
        }
    },
    sum_of_squares(true) {
        @Override
        public <T> T visit(StatVisitor<T> visitor) {
            return visitor.visitSumOfSquares();
        }
    },
    variance(true) {
        @Override
        public <T> T visit(StatVisitor<T> visitor) {
            return visitor.visitVariance();
        }
    },
    std_deviation(true) {
        @Override
        public <T> T visit(StatVisitor<T> visitor) {
            return visitor.visitStdDeviation();
        }
    };

    public boolean extended;

    Stat(boolean extended) {
        this.extended = extended;
    }

    public boolean isExtended() {
        return extended;
    }


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

    public abstract <T> T visit(StatVisitor<T> visitor);
}
