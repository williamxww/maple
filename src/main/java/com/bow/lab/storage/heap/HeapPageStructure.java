package com.bow.lab.storage.heap;

import com.bow.lab.storage.IPageStructure;
import com.bow.maple.relations.ColumnInfo;
import com.bow.maple.relations.Tuple;
import com.bow.maple.storage.DBPage;
import com.bow.maple.storage.TableFileInfo;
import com.bow.maple.storage.heapfile.DataPage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * slot从前往后增加，tuple从页末往前增长。 slot1中存储了tuple1的偏移量。
 * 
 * <pre>
 * |  2 B   | 2 B | 2 B | 2 B |
 * |numSlots|slot1|slot2|slot3|...
 * .............
 * |tuple3|tuple2|tuple1|
 * </pre>
 * 
 * slotVal中存放每个tuple的偏移量
 * 
 * @author vv
 * @since 2017/11/21.
 */
public class HeapPageStructure implements IPageStructure {

    private static Logger logger = LoggerFactory.getLogger(DataPage.class);

    private static final int OFFSET_NUM_SLOTS = 0;

    public static final int EMPTY_SLOT = 0;

    @Override
    public void initNewPage(DBPage dbPage) {
        setNumSlots(dbPage, 0);
    }

    /**
     * 获取slot的偏移量
     *
     * <pre>
     *     |  2 B   | 2 B | 2 B |...
     *     |numSlots|slot1|slot2|...
     * </pre>
     *
     * @param slot slot
     * @return slot的偏移量
     */
    @Override
    public int getSlotOffset(int slot) {
        return (1 + slot) * 2;
    }

    /**
     * 获取总slot 数量
     * 
     * @return 总slot数量
     */
    public int getNumSlots(DBPage dbPage) {
        return dbPage.readUnsignedShort(OFFSET_NUM_SLOTS);
    }

    public void setNumSlots(DBPage dbPage, int numSlots) {
        dbPage.writeShort(OFFSET_NUM_SLOTS, numSlots);
    }

    /**
     * 获取指定slot的值，即tuple的起始偏移位置或是{@link #EMPTY_SLOT}
     * 
     * @param slot slot
     * @return 对应tuple的偏移量
     */
    public int getSlotValue(DBPage dbPage, int slot) {
        int numSlots = getNumSlots(dbPage);
        if (slot < 0 || slot >= numSlots) {
            throw new IllegalArgumentException("Valid slots are in range [0," + numSlots + ").  Got " + slot);
        }
        return dbPage.readUnsignedShort(getSlotOffset(slot));
    }

    /**
     * 将值（各个tuple的offset）放到slot中，
     * 
     * @param slot slot
     * @param value tuple的偏移量
     */
    public void setSlotValue(DBPage dbPage, int slot, int value) {
        int numSlots = getNumSlots(dbPage);
        if (slot < 0 || slot >= numSlots) {
            throw new IllegalArgumentException("Valid slots are in range [0," + numSlots + ").  Got " + slot);
        }
        dbPage.writeShort(getSlotOffset(slot), value);
    }

    /**
     * 获取slot list结束的位置
     * 
     * @return slot区域结束的位置
     */
    public int getSlotsEndIndex(DBPage dbPage) {
        return getSlotOffset(getNumSlots(dbPage));
    }

    /**
     * 将offset转换为slot
     * 
     * @param offset 在整个page中的offset
     * @return slot
     * @throws IllegalArgumentException e
     */
    public int getSlotIndexFromOffset(DBPage dbPage, int offset) throws IllegalArgumentException {
        if (offset % 2 != 0) {
            throw new IllegalArgumentException("Slots occur at even indexes (each slot is a short).");
        }
        int slot = (offset - 2) / 2;
        int numSlots = getNumSlots(dbPage);
        if (slot < 0 || slot >= numSlots) {
            throw new IllegalArgumentException("Valid slots are in range [0," + numSlots + ").  Got " + slot);
        }
        return slot;
    }

    /**
     * 获取tuple数据的起始位置。slot list中最后一个slot存放的是第一个的tuple的位置（放在页尾）。
     * 
     * @return 起始位置
     */
    public int getTupleDataStart(DBPage dbPage) {
        int numSlots = getNumSlots(dbPage);
        // 数据区起始位置默认页的末尾，最后一个slot里有值则用其值。
        int dataStart = getTupleDataEnd(dbPage);
        int slot = numSlots - 1;
        while (slot >= 0) {
            // 获取最后一个slot中存放的tuple偏移量
            int slotValue = getSlotValue(dbPage, slot);
            if (slotValue != EMPTY_SLOT) {
                // 只要对应的tuple没有被删除，此值就是首个tuple的起始位置
                dataStart = slotValue;
                break;
            }
            --slot;
        }
        return dataStart;
    }

    /**
     * tuple数据的结束位置，数据页页末。
     * 
     * @return tuple数据结束位置
     */
    public int getTupleDataEnd(DBPage dbPage) {
        return dbPage.getPageSize();
    }

    /**
     * 获取指定tuple的长度. slot list中最后一个slot存放的是首个tuple的offset,前一个slot里存放着下一个
     * 
     * @param slot 指定某个tuple
     * @return tuple长度
     */
    @Override
    public int getTupleLength(DBPage dbPage, int slot) {
        int numSlots = getNumSlots(dbPage);
        if (slot < 0 || slot >= numSlots) {
            throw new IllegalArgumentException("Valid slots are in range [0," + slot + ").  Got " + slot);
        }

        int tupleStart = getSlotValue(dbPage, slot);
        if (tupleStart == EMPTY_SLOT) {
            // 被删除了
            throw new IllegalArgumentException("Slot " + slot + " is empty.");
        }

        int tupleLength = -1;
        // 前一个slot里放的是下一个tuple的offset
        int prevSlot = slot - 1;
        while (prevSlot >= 0) {
            int prevTupleStart = getSlotValue(dbPage, prevSlot);
            if (prevTupleStart != EMPTY_SLOT) {
                // Earlier slots have higher offsets. (Yes it's weird.)
                tupleLength = prevTupleStart - tupleStart;
                break;
            }
            prevSlot--;
        }

        if (prevSlot < 0) {
            // 此slot放着此页中最后一个tuple的offset
            tupleLength = getTupleDataEnd(dbPage) - tupleStart;
        }
        return tupleLength;
    }

    /**
     * 获取剩余空间
     * 
     * @return 剩余空间
     */
    @Override
    public int getFreeSpaceInPage(DBPage dbPage) {
        return getTupleDataStart(dbPage) - getSlotsEndIndex(dbPage);
    }

    /**
     * 合理性检查，后面slot存放的offset应该小于前面的
     */
    @Override
    public void sanityCheck(DBPage dbPage) {
        // 一个tuple都没有
        int numSlots = getNumSlots(dbPage);
        if (numSlots == 0) {
            return;
        }

        // 获取第一个有效的slot,取出slot和offset放到prevSlot,prevOffset
        int iSlot = -1;
        int prevSlot = -1;
        int prevOffset = EMPTY_SLOT;
        while (iSlot + 1 < numSlots && prevOffset == EMPTY_SLOT) {
            iSlot++;
            prevSlot = iSlot;
            prevOffset = getSlotValue(dbPage, iSlot);
        }

        // 循环检查后面slot存放的offset应该小于前面的
        while (iSlot + 1 < numSlots) {
            int offset = EMPTY_SLOT;
            while (iSlot + 1 < numSlots && offset == EMPTY_SLOT) {
                // 找到下一个有效的slot，取出值(前一个tuple的offset)
                iSlot++;
                offset = getSlotValue(dbPage, iSlot);
            }

            if (iSlot < numSlots) {
                // 当前slot存放的值大于前slot存放的值，就有问题了
                if (offset >= prevOffset) {
                    logger.warn("Slot {} and {} offsets are not strictly decreasing ({} should be greater than {})",
                            prevSlot, iSlot, prevOffset, offset);
                }
                prevSlot = iSlot;
                prevOffset = offset;
            }
        }
    }

    /**
     * 从off向前开辟len区域.若off前面有数据了，则把这块数据[tupDataStart, off)整体往前推len。
     * 
     * @param off 指定位置
     * @param len 开辟区域长度
     */
    @Override
    public void insertTupleDataRange(DBPage dbPage, int off, int len) {

        int tupDataStart = getTupleDataStart(dbPage);
        if (off < tupDataStart) {
            throw new IllegalArgumentException(
                    "Specified offset " + off + " is not actually in the tuple data portion of this page "
                            + "(data starts at offset " + tupDataStart + ").");
        }
        if (len < 0) {
            throw new IllegalArgumentException("Length must not be negative.");
        }
        if (len > getFreeSpaceInPage(dbPage)) {
            throw new IllegalArgumentException("Specified length " + len
                    + " is larger than amount of free space in this page (" + getFreeSpaceInPage(dbPage) + " bytes).");
        }

        // 如果off本来就是tupDataStart就不用移动数据了
        if (off > tupDataStart) {
            // 把[tupDataStart, off)向前移动len即移动到[tupDataStart - len, off - len)
            // [off - len, off)就空出来了
            dbPage.moveDataRange(tupDataStart, tupDataStart - len, off - tupDataStart);
        }

        // 向前清理出一片len长度的区域
        int startOff = off - len;
        dbPage.setDataRange(startOff, len, (byte) 0);

        // 偏移量小于off的tuple其slot中的offset要减去len
        int numSlots = getNumSlots(dbPage);
        for (int iSlot = 0; iSlot < numSlots; iSlot++) {
            int slotOffset = getSlotValue(dbPage, iSlot);
            if (slotOffset != EMPTY_SLOT && slotOffset < off) {
                // Update this slot's offset.
                slotOffset -= len;
                setSlotValue(dbPage, iSlot, slotOffset);
            }
        }
    }

    /**
     * 从指定位置开始删除len长度的数据。 把数据区[tupDataStart, off) 移动到[tupDataStart + len, off +
     * len)
     * 
     * @param off 指定位置
     * @param len 要删除的数据长度
     */
    @Override
    public void deleteTupleDataRange(DBPage dbPage, int off, int len) {
        int tupDataStart = getTupleDataStart(dbPage);
        logger.debug("Deleting tuple data-range offset {}, length {}", off, len);
        if (off < tupDataStart) {
            throw new IllegalArgumentException(
                    "Specified offset " + off + " is not actually in the tuple data portion of this page "
                            + "(data starts at offset " + tupDataStart + ").");
        }
        if (len < 0)
            throw new IllegalArgumentException("Length must not be negative.");

        if (getTupleDataEnd(dbPage) - off < len) {
            throw new IllegalArgumentException(
                    "Specified length " + len + " is larger than size of tuple data in this page ("
                            + (getTupleDataEnd(dbPage) - off) + " bytes).");
        }

        // 把数据区[tupDataStart, off) 移动到[tupDataStart + len, off + len).
        logger.debug("Moving {} bytes of data from [{}, {}) to [{}, {})", off - tupDataStart, tupDataStart, off,
                tupDataStart + len, off + len);
        dbPage.moveDataRange(tupDataStart, tupDataStart + len, off - tupDataStart);

        // offset小于off的tuple对应的slot值都需要更新(往后移动了len)
        int numSlots = getNumSlots(dbPage);
        for (int iSlot = 0; iSlot < numSlots; iSlot++) {
            int slotOffset = dbPage.readUnsignedShort(2 * (iSlot + 1));
            if (slotOffset != EMPTY_SLOT && slotOffset <= off) {
                // Update this slot's offset.
                slotOffset += len;
                setSlotValue(dbPage, iSlot, slotOffset);
            }
        }
    }

    /**
     * 分配一个长度为len的空间给新tuple. 分配时先循环已有的slot看看有没有空的slot，若没有再分配一个新的。
     * 
     * @param len 空间长度
     * @return 新tuple对应的slot号
     */
    @Override
    public int allocNewTuple(DBPage dbPage, int len) {
        if (len < 0) {
            throw new IllegalArgumentException("Length must be nonnegative; got " + len);
        }
        int spaceNeeded = len;
        logger.debug("Allocating space for new " + len + "-byte tuple.");

        // 循环每个slot发现有可用的slot则用之
        int slot;
        int numSlots = getNumSlots(dbPage);
        logger.debug("Current number of slots on page:  " + numSlots);
        int newTupleEnd = getTupleDataEnd(dbPage);
        for (slot = 0; slot < numSlots; slot++) {
            int currSlotValue = getSlotValue(dbPage, slot);
            if (currSlotValue == EMPTY_SLOT) {
                break;
            } else {
                // 新tuple的end就是前一个tuple的offset
                newTupleEnd = currSlotValue;
            }
        }

        // 没有可用的，就新开辟一个slot(2B),因此所需空间要+2
        if (slot == numSlots) {
            spaceNeeded += 2;
        }
        if (spaceNeeded > getFreeSpaceInPage(dbPage)) {
            throw new IllegalArgumentException("Space needed for new tuple (" + spaceNeeded
                    + " bytes) is larger than the free space in this page (" + getFreeSpaceInPage(dbPage) + " bytes).");
        }
        if (slot == numSlots) {
            // 到此处说明没有空slot，要新开辟一个slot(2B)
            logger.debug("No empty slot available.  Adding a new slot.");
            numSlots++;
            setNumSlots(dbPage, numSlots);
            setSlotValue(dbPage, slot, EMPTY_SLOT);
        }
        logger.debug("Tuple will get slot {}.  Final number of slots:  {}", slot, numSlots);

        // 找到新tuple的起始位置
        int newTupleStart = newTupleEnd - len;
        logger.debug("New tuple of {} bytes will reside at location [{}, {}).", len, newTupleStart, newTupleEnd);

        // 从newTupleEnd往前开辟len长的空间
        insertTupleDataRange(dbPage, newTupleEnd, len);
        // 设置新slot对应的新tuple的偏移量
        setSlotValue(dbPage, slot, newTupleStart);
        // 返回slot号
        return slot;
    }

    @Override
    public HeapPageTuple storeNewTuple(TableFileInfo tblInfo, DBPage dbPage, int slot, int pageOffset, Tuple tuple) {
        List<ColumnInfo> colInfos = tblInfo.getSchema().getColumnInfos();
        PageTupleUtil.storeTuple(dbPage, pageOffset, colInfos, tuple);
        return new HeapPageTuple(tblInfo, dbPage, slot, pageOffset);
    }

    /**
     * 删除slot对应的tuple.将此slot标记为empty,然后通过移动数据块的方式释放掉tuple的区域
     * 
     * @param slot slot号
     */
    @Override
    public void deleteTuple(DBPage dbPage, int slot) {

        // 参数校验
        if (slot < 0) {
            throw new IllegalArgumentException("Slot must be nonnegative; got " + slot);
        }
        int numSlots = getNumSlots(dbPage);
        if (slot >= numSlots) {
            throw new IllegalArgumentException(
                    "Page only has " + numSlots + " slots, but slot " + slot + " was requested for deletion.");
        }

        int tupleStart = getSlotValue(dbPage, slot);
        if (tupleStart == EMPTY_SLOT) {
            // 已经被删除了，抛出异常
            throw new IllegalArgumentException(
                    "Slot " + slot + " was requested for deletion, but it is already deleted.");
        }

        int tupleLength = getTupleLength(dbPage, slot);

        // 删除对应数据并将slot标记为EMPTY_SLOT
        logger.debug("Deleting tuple page {}, slot {} with starting offset {}, length {}.", dbPage.getPageNo(), slot,
                tupleStart, tupleLength);
        // 释放对应的数据空间
        deleteTupleDataRange(dbPage, tupleStart, tupleLength);
        setSlotValue(dbPage, slot, EMPTY_SLOT);

        // 处在slot list末尾的slot若是EMPTY_SLOT则删除(只要总数减去即可)
        boolean numSlotsChanged = false;
        for (slot = numSlots - 1; slot >= 0; slot--) {
            int currSlotValue = getSlotValue(dbPage, slot);
            if (currSlotValue != EMPTY_SLOT) {
                break;
            }
            numSlots--;
            numSlotsChanged = true;
        }
        if (numSlotsChanged) {
            // 更新总数，变相的删除了处在末尾的EMPTY_SLOT
            setNumSlots(dbPage, numSlots);
        }
    }

}
