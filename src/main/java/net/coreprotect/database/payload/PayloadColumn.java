package net.coreprotect.database.payload;

public class PayloadColumn {

    private final String inlineColumn;
    private final String payloadIdColumn;

    public PayloadColumn(String inlineColumn, String payloadIdColumn) {
        this.inlineColumn = inlineColumn;
        this.payloadIdColumn = payloadIdColumn;
    }

    public String inlineColumn() {
        return inlineColumn;
    }

    public String payloadIdColumn() {
        return payloadIdColumn;
    }
}
