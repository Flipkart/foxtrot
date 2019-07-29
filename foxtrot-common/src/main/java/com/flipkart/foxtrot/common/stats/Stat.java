package com.flipkart.foxtrot.common.stats;

public enum Stat {

    COUNT(false) {
        @Override
        public <T> T visit(StatVisitor<T> visitor) {
            return visitor.visitCount();
        }
    },
    MIN(false) {
        @Override
        public <T> T visit(StatVisitor<T> visitor) {
            return visitor.visitMin();
        }
    },
    MAX(false) {
        @Override
        public <T> T visit(StatVisitor<T> visitor) {
            return visitor.visitMax();
        }
    },
    AVG(false) {
        @Override
        public <T> T visit(StatVisitor<T> visitor) {
            return visitor.visitAvg();
        }
    },
    SUM(false) {
        @Override
        public <T> T visit(StatVisitor<T> visitor) {
            return visitor.visitSum();
        }
    },
    SUM_OF_SQUARES(true) {
        @Override
        public <T> T visit(StatVisitor<T> visitor) {
            return visitor.visitSumOfSquares();
        }
    },
    VARIANCE(true) {
        @Override
        public <T> T visit(StatVisitor<T> visitor) {
            return visitor.visitVariance();
        }
    },
    STD_DEVIATION(true) {
        @Override
        public <T> T visit(StatVisitor<T> visitor) {
            return visitor.visitStdDeviation();
        }
    };

    private boolean extended;

    Stat(boolean extended) {
        this.extended = extended;
    }

    public boolean isExtended() {
        return extended;
    }

    public abstract <T> T visit(StatVisitor<T> visitor);

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
}
