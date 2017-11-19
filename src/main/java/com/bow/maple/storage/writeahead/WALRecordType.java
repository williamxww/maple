package com.bow.maple.storage.writeahead;

/**
 * 枚举类id从0x51开始，是因为5*16是整数。最好高位不要是1(如0xF1)，高位为1的byte在转int时，高位全补1导致值变了，处理麻烦。
 */
public enum WALRecordType {
    START_TXN(0x51),

    UPDATE_PAGE(0x52),

    UPDATE_PAGE_REDO_ONLY(0x53),

    COMMIT_TXN(0x5C),

    ABORT_TXN(0x5A);

    private int id;

    private WALRecordType(int id) {
        this.id = id;
    }

    public int getID() {
        return id;
    }

    public static WALRecordType valueOf(int id) {
        for (WALRecordType type : values()) {
            if (type.id == id)
                return type;
        }
        return null;
    }
}
