package com.bow.lab.storage.heap;

import com.bow.lab.storage.IPage;
import com.bow.maple.storage.DBPage;
import com.bow.maple.storage.heapfile.DataPage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author vv
 * @since 2017/11/21.
 */
public class HeapPage implements IPage {

    private static Logger logger = LoggerFactory.getLogger(DataPage.class);

    public static final int OFFSET_NUM_SLOTS = 0;

    public static final int EMPTY_SLOT = 0;

    private DBPage dbPage;

    public HeapPage(DBPage dbPage) {
        this.dbPage = dbPage;
    }

    public int getNumSlots() {
        return dbPage.readUnsignedShort(OFFSET_NUM_SLOTS);
    }

    public void setNumSlots(int numSlots) {
        dbPage.writeShort(OFFSET_NUM_SLOTS, numSlots);
    }

    public int getSlotsEndIndex() {
        return getSlotOffset(getNumSlots());
    }

    public int getSlotValue(int slot) {
        int numSlots = getNumSlots();
        if (slot < 0 || slot >= numSlots) {
            throw new IllegalArgumentException("Valid slots are in range [0," + numSlots + ").  Got " + slot);
        }
        return dbPage.readUnsignedShort(getSlotOffset(slot));
    }

    /**
     * 将值（各个tuple的offset）放到slot中，
     * 
     * @param slot
     * @param value
     */
    public void setSlotValue(int slot, int value) {
        int numSlots = getNumSlots();

        if (slot < 0 || slot >= numSlots) {
            throw new IllegalArgumentException("Valid slots are in range [0," + numSlots + ").  Got " + slot);
        }

        dbPage.writeShort(getSlotOffset(slot), value);
    }

    public int getSlotIndexFromOffset(int offset) throws IllegalArgumentException {

        if (offset % 2 != 0) {
            throw new IllegalArgumentException("Slots occur at even indexes (each slot is a short).");
        }

        int slot = (offset - 2) / 2;
        int numSlots = getNumSlots();

        if (slot < 0 || slot >= numSlots) {
            throw new IllegalArgumentException("Valid slots are in range [0," + numSlots + ").  Got " + slot);
        }

        return slot;
    }

    public int getTupleDataStart() {
        int numSlots = getNumSlots();

        // If there are no tuples in this page, "data start" is the top of the
        // page data.
        int dataStart = getTupleDataEnd();

        int slot = numSlots - 1;
        while (slot >= 0) {
            int slotValue = getSlotValue(slot);
            if (slotValue != EMPTY_SLOT) {
                dataStart = slotValue;
                break;
            }

            --slot;
        }

        return dataStart;
    }

    public int getTupleDataEnd() {
        return dbPage.getPageSize();
    }

    @Override
    public int getTupleLength(int slot) {
        int numSlots = getNumSlots();

        if (slot < 0 || slot >= numSlots) {
            throw new IllegalArgumentException("Valid slots are in range [0," + slot + ").  Got " + slot);
        }

        int tupleStart = getSlotValue(slot);

        if (tupleStart == EMPTY_SLOT)
            throw new IllegalArgumentException("Slot " + slot + " is empty.");

        int tupleLength = -1;

        int prevSlot = slot - 1;
        while (prevSlot >= 0) {
            int prevTupleStart = getSlotValue(prevSlot);
            if (prevTupleStart != EMPTY_SLOT) {

                // Earlier slots have higher offsets. (Yes it's weird.)
                tupleLength = prevTupleStart - tupleStart;

                break;
            }

            prevSlot--;
        }

        if (prevSlot < 0) {
            // The specified slot held the last tuple in the page.
            tupleLength = getTupleDataEnd() - tupleStart;
        }

        return tupleLength;
    }

    @Override
    public int getFreeSpaceInPage() {
        return getTupleDataStart() - getSlotsEndIndex();
    }

    @Override
    public void sanityCheck() {
        int numSlots = getNumSlots();
        if (numSlots == 0)
            return;

        // Find the first occupied slot, and get its offset into prevOffset.
        int iSlot = -1;
        int prevSlot = -1;
        int prevOffset = EMPTY_SLOT;
        while (iSlot + 1 < numSlots && prevOffset == EMPTY_SLOT) {
            iSlot++;
            prevSlot = iSlot;
            prevOffset = getSlotValue(iSlot);
        }

        while (iSlot + 1 < numSlots) {

            // Find the next occupied slot, and get its offset into offset.
            int offset = EMPTY_SLOT;
            while (iSlot + 1 < numSlots && offset == EMPTY_SLOT) {
                iSlot++;
                offset = getSlotValue(iSlot);
            }

            if (iSlot < numSlots) {
                // Tuple offsets should be strictly decreasing.
                if (prevOffset <= offset) {
                    logger.warn(String.format(
                            "Slot %d and %d offsets are not strictly decreasing " + "(%d should be greater than %d)",
                            prevSlot, iSlot, prevOffset, offset));
                }

                prevSlot = iSlot;
                prevOffset = offset;
            }
        }
    }

    @Override
    public void insertTupleDataRange(int off, int len) {

        int tupDataStart = getTupleDataStart();

        if (off < tupDataStart) {
            throw new IllegalArgumentException(
                    "Specified offset " + off + " is not actually in the tuple data portion of this page "
                            + "(data starts at offset " + tupDataStart + ").");
        }

        if (len < 0)
            throw new IllegalArgumentException("Length must not be negative.");

        if (len > getFreeSpaceInPage()) {
            throw new IllegalArgumentException("Specified length " + len
                    + " is larger than amount of free space in this page (" + getFreeSpaceInPage() + " bytes).");
        }

        // If off == tupDataStart then there's no need to move anything.
        if (off > tupDataStart) {
            // Move the data in the range [tupDataStart, off) to
            // [tupDataStart - len, off - len). Thus there will be a gap in the
            // range [off - len, off) after the operation is completed.

            dbPage.moveDataRange(tupDataStart, tupDataStart - len, off - tupDataStart);
        }

        // Zero out the gap that was just created.
        int startOff = off - len;
        dbPage.setDataRange(startOff, len, (byte) 0);

        // Update affected slots; this includes all slots below the specified
        // offset. The update is easy; slot values just move down by len bytes.

        int numSlots = getNumSlots();
        for (int iSlot = 0; iSlot < numSlots; iSlot++) {

            int slotOffset = getSlotValue(iSlot);
            if (slotOffset != EMPTY_SLOT && slotOffset < off) {
                // Update this slot's offset.
                slotOffset -= len;
                setSlotValue(iSlot, slotOffset);
            }
        }
    }

    @Override
    public void deleteTupleDataRange(int off, int len) {
        int tupDataStart = getTupleDataStart();

        logger.debug(String.format("Deleting tuple data-range offset %d, length %d", off, len));

        if (off < tupDataStart) {
            throw new IllegalArgumentException(
                    "Specified offset " + off + " is not actually in the tuple data portion of this page "
                            + "(data starts at offset " + tupDataStart + ").");
        }

        if (len < 0)
            throw new IllegalArgumentException("Length must not be negative.");

        if (getTupleDataEnd() - off < len) {
            throw new IllegalArgumentException("Specified length " + len
                    + " is larger than size of tuple data in this page (" + (getTupleDataEnd() - off) + " bytes).");
        }

        // Move the data in the range [tupDataStart, off) to
        // [tupDataStart + len, off + len).

        logger.debug(String.format("    Moving %d bytes of data from [%d, %d) to [%d, %d)", off - tupDataStart,
                tupDataStart, off, tupDataStart + len, off + len));

        dbPage.moveDataRange(tupDataStart, tupDataStart + len, off - tupDataStart);

        // Update affected slots; this includes all (non-empty) slots whose
        // offset is below the specified offset. The update is easy; slot
        // values just move up by len bytes.

        int numSlots = getNumSlots();
        for (int iSlot = 0; iSlot < numSlots; iSlot++) {

            int slotOffset = dbPage.readUnsignedShort(2 * (iSlot + 1));
            if (slotOffset != EMPTY_SLOT && slotOffset <= off) {
                // Update this slot's offset.
                slotOffset += len;
                setSlotValue(iSlot, slotOffset);
            }
        }
    }

    @Override
    public int allocNewTuple(int len) {

        if (len < 0) {
            throw new IllegalArgumentException("Length must be nonnegative; got " + len);
        }

        // The amount of free space we need in the database page, if we are
        // going to add the new tuple.
        int spaceNeeded = len;

        logger.debug("Allocating space for new " + len + "-byte tuple.");

        // Search through the current list of slots in the page. If a slot
        // is marked as "empty" then we can use that slot. Otherwise, we
        // will need to add a new slot to the end of the list.

        int slot;
        int numSlots = getNumSlots();

        logger.debug("Current number of slots on page:  " + numSlots);

        // This variable tracks where the new tuple should END. It starts
        // as the page-size, and gets moved down past each valid tuple in
        // the page, until we find an available slot in the page.
        int newTupleEnd = getTupleDataEnd();

        for (slot = 0; slot < numSlots; slot++) {
            // currSlotValue is either the start of that slot's tuple-data,
            // or it is set to EMPTY_SLOT.
            int currSlotValue = getSlotValue(slot);

            if (currSlotValue == EMPTY_SLOT)
                break;
            else
                newTupleEnd = currSlotValue;
        }

        // First make sure we actually have enough space for the new tuple.

        if (slot == numSlots) {
            // We'll need to add a new slot to the list. Make sure there's
            // room.
            spaceNeeded += 2;
        }

        if (spaceNeeded > getFreeSpaceInPage()) {
            // Switch this to a checked exception? The table manager has
            // already verified that the page should have enough space, so if
            // this fails, it would indicate a bug. So, runtime exception is
            // fine for now.
            throw new IllegalArgumentException("Space needed for new tuple (" + spaceNeeded
                    + " bytes) is larger than the free space in this page (" + getFreeSpaceInPage() + " bytes).");
        }

        // Now we know we have space for the tuple. Update the slot list,
        // and the update page's layout to make room for the new tuple.

        if (slot == numSlots) {
            logger.debug("No empty slot available.  Adding a new slot.");

            // Add the new slot to the page, and update the total number of
            // slots.
            numSlots++;
            setNumSlots(numSlots);
            setSlotValue(slot, EMPTY_SLOT);
        }

        logger.debug(String.format("Tuple will get slot %d.  Final number of slots:  %d", slot, numSlots));

        int newTupleStart = newTupleEnd - len;

        logger.debug(String.format("New tuple of %d bytes will reside at location [%d, %d).", len, newTupleStart,
                newTupleEnd));

        // Make room for the new tuple's data to be stored into. Since
        // tuples are stored from the END of the page going backwards, we
        // specify the new tuple's END index, and the tuple's length.
        // (Note: This call also updates all affected slots whose offsets
        // would be changed.)
        insertTupleDataRange(newTupleEnd, len);

        // Set the slot's value to be the starting offset of the tuple.
        // We have to do this *after* we insert the new space for the new
        // tuple, or else insertTupleDataRange() will clobber the
        // slot-value of this tuple.
        setSlotValue(slot, newTupleStart);

        // Finally, return the slot-index of the new tuple.
        return slot;
    }

    @Override
    public void deleteTuple(int slot) {

        if (slot < 0) {
            throw new IllegalArgumentException("Slot must be nonnegative; got " + slot);
        }

        int numSlots = getNumSlots();

        if (slot >= numSlots) {
            throw new IllegalArgumentException(
                    "Page only has " + numSlots + " slots, but slot " + slot + " was requested for deletion.");
        }

        // Get the tuple's offset and length.
        int tupleStart = getSlotValue(slot);
        if (tupleStart == EMPTY_SLOT) {
            throw new IllegalArgumentException(
                    "Slot " + slot + " was requested for deletion, but it is already deleted.");
        }

        int tupleLength = getTupleLength(slot);

        // Mark the slot's entry as empty, and clear out the tuple's space.

        logger.debug(String.format("Deleting tuple page %d, slot %d with starting offset %d, length %d.",
                dbPage.getPageNo(), slot, tupleStart, tupleLength));

        deleteTupleDataRange(tupleStart, tupleLength);
        setSlotValue(slot, EMPTY_SLOT);

        // Finally, release all empty slots at the end of the slot-list.

        boolean numSlotsChanged = false;
        for (slot = numSlots - 1; slot >= 0; slot--) {
            // currSlotValue is either the start of that slot's tuple-data,
            // or it is set to EMPTY_SLOT.
            int currSlotValue = getSlotValue(slot);

            if (currSlotValue != EMPTY_SLOT)
                break;

            numSlots--;
            numSlotsChanged = true;
        }

        if (numSlotsChanged)
            setNumSlots(numSlots);
    }

    public static int getSlotOffset(int slot) {
        return (1 + slot) * 2;
    }
}
