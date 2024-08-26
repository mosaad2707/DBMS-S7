#include "BlockBuffer.h"
#include <cstdlib>
#include <cstring>
#include <stdio.h>

// Function to compare two attributes (either NUMBER or STRING types).
int compareAttrs(union Attribute attr1, union Attribute attr2, int attrType) {
    int diff;
    (attrType == NUMBER)
        ? diff = attr1.nVal - attr2.nVal // Compare numerical values if type is NUMBER.
        : diff = strcmp(attr1.sVal, attr2.sVal); // Compare string values if type is STRING.
    if (diff > 0)
        return 1; // attr1 > attr2
    else if (diff < 0)
        return -1; // attr1 < attr2
    else 
        return 0; // attr1 == attr2
}

// Constructor to initialize BlockBuffer with a given block number.
BlockBuffer::BlockBuffer(int blockNum) {
    this->blockNum = blockNum;
}

// Constructor to initialize BlockBuffer with a block type and find a free block of that type.
BlockBuffer::BlockBuffer(char blockType) {
    int blockTypeNum;
    if (blockType == 'R')
        blockTypeNum = REC;
    else if (blockType == 'I')
        blockTypeNum = IND_INTERNAL;
    else if (blockType == 'L')
        blockTypeNum = IND_LEAF;
    else 
        blockTypeNum = UNUSED_BLK;
    int blockNum = getFreeBlock(blockTypeNum);

    this->blockNum = blockNum;
    if (blockNum < 0 || blockNum >= DISK_BLOCKS)
        return; // Invalid block number.
}

// Constructor to initialize RecBuffer with a given block number by calling BlockBuffer constructor.
RecBuffer::RecBuffer(int blockNum) : BlockBuffer::BlockBuffer(blockNum) {}

// Default constructor to initialize RecBuffer with a block of type 'R' (Record).
RecBuffer::RecBuffer() : BlockBuffer::BlockBuffer('R') {}

// Function to retrieve the header information of the block into the provided HeadInfo structure.
int BlockBuffer::getHeader(struct HeadInfo* head) {
    unsigned char* bufferPtr;
    int ret = loadBlockAndGetBufferPtr(&bufferPtr);    

    if (ret != SUCCESS)
        return ret; // Return error if unable to load block.

    // Copy the header information from the buffer to the provided HeadInfo structure.
    HeadInfo* header = (HeadInfo*) bufferPtr;
    head->numSlots = header->numSlots;
    head->numEntries = header->numEntries;
    head->numAttrs = header->numAttrs;
    head->lblock = header->lblock;
    head->rblock = header->rblock;

    return SUCCESS;
}

// Function to set the header information of the block from the provided HeadInfo structure.
int BlockBuffer::setHeader(struct HeadInfo* head) {
    unsigned char* bufferPtr;
    int ret = loadBlockAndGetBufferPtr(&bufferPtr);

    if (ret != SUCCESS)
        return ret; // Return error if unable to load block.

    // Copy the provided header information into the block buffer.
    HeadInfo* header = (HeadInfo*) bufferPtr;
    header->numSlots = head->numSlots;
    header->numEntries = head->numEntries;
    header->numAttrs = head->numAttrs;
    header->lblock = head->lblock;
    header->rblock = head->rblock;

    return StaticBuffer::setDirtyBit(this->blockNum); // Mark the block as dirty.
}

// Function to retrieve a record from the block at the specified slot number.
int RecBuffer::getRecord(union Attribute* rec, int slotNum) {
    struct HeadInfo head;

    this->getHeader(&head); // Retrieve the header information.

    int attrCount = head.numAttrs;
    int slotCount = head.numSlots;

    unsigned char* bufferPtr;
    int ret = loadBlockAndGetBufferPtr(&bufferPtr);
    if (ret != SUCCESS)
        return ret; // Return error if unable to load block.

    // Calculate the record's position within the block based on the slot number.
    int recordSize = attrCount * ATTR_SIZE;
    unsigned char* slotPointer = bufferPtr + 32 + slotCount + recordSize * slotNum;
    memcpy(rec, slotPointer, recordSize); // Copy the record data into the provided attribute union.
    return SUCCESS;
}

// Function to set a record in the block at the specified slot number.
int RecBuffer::setRecord(union Attribute* rec, int slotNum) {
    unsigned char* bufferPtr;

    int ret = loadBlockAndGetBufferPtr(&bufferPtr);

    if (ret != SUCCESS)
        return ret; // Return error if unable to load block.

    HeadInfo header;
    this->getHeader(&header); // Retrieve the header information.

    int numAttrs = header.numAttrs;
    int numSlots = header.numSlots;

    if (slotNum < 0 || slotNum >= numSlots)
        return E_OUTOFBOUND; // Return error if slot number is out of bounds.
    
    // Calculate the record's position within the block based on the slot number.
    int recordSize = numAttrs * ATTR_SIZE;
    unsigned char* recordPtr = bufferPtr + HEADER_SIZE + numSlots + slotNum * recordSize;

    memcpy(recordPtr, rec, recordSize); // Copy the record data into the block buffer.
    StaticBuffer::setDirtyBit(this->blockNum); // Mark the block as dirty.

    return SUCCESS;
}

// Function to retrieve the slot map from the block buffer.
int RecBuffer::getSlotMap(unsigned char* slotMap) {
    unsigned char* bufferPtr;

    int ret = loadBlockAndGetBufferPtr(&bufferPtr);

    if (ret != SUCCESS)
        return ret; // Return error if unable to load block.

    struct HeadInfo head;
    getHeader(&head); // Retrieve the header information.

    int slotCount = head.numSlots;

    // Copy the slot map from the block buffer into the provided slotMap array.
    unsigned char* slotMapInBuffer = bufferPtr + HEADER_SIZE;
    memcpy(slotMap, slotMapInBuffer, slotCount);

    return SUCCESS;
}

// Function to set the slot map in the block buffer.
int RecBuffer::setSlotMap(unsigned char* slotMap) {
    unsigned char* bufferPtr;

    int ret = loadBlockAndGetBufferPtr(&bufferPtr);

    if (ret != SUCCESS)
        return ret; // Return error if unable to load block.

    struct HeadInfo head;
    getHeader(&head); // Retrieve the header information.

    int slotCount = head.numSlots;

    // Copy the provided slot map into the block buffer.
    unsigned char* slotMapInBuffer = bufferPtr + HEADER_SIZE;
    memcpy(slotMapInBuffer, slotMap, slotCount);

    return StaticBuffer::setDirtyBit(this->blockNum); // Mark the block as dirty.
}

// Function to load a block into memory and get the pointer to the buffer.
int BlockBuffer::loadBlockAndGetBufferPtr(unsigned char** bufferPtr) {
    int bufferNum = StaticBuffer::getBufferNum(this->blockNum);

    if (bufferNum == E_BLOCKNOTINBUFFER) {
        // If the block is not already in buffer, find a free buffer and read the block from disk.
        bufferNum = StaticBuffer::getFreeBuffer(this->blockNum);

        if (bufferNum == E_OUTOFBOUND)
            return E_OUTOFBOUND; // Return error if no free buffer is available.

        Disk::readBlock(StaticBuffer::blocks[bufferNum], this->blockNum); // Read block from disk.
    }
    else {
        // If the block is already in buffer, update the timestamps for LRU (Least Recently Used) management.
        for (int i = 0; i < BUFFER_CAPACITY; i++) {
            if (!StaticBuffer::metainfo[i].free)
                StaticBuffer::metainfo[i].timeStamp++;
        }

        StaticBuffer::metainfo[bufferNum].timeStamp = 0; // Reset timestamp for the current buffer.
    }

    *bufferPtr = StaticBuffer::blocks[bufferNum]; // Set the buffer pointer to the loaded block.

    return SUCCESS;
}

// Function to get the block number associated with this BlockBuffer.
int BlockBuffer::getBlockNum() {
    return this->blockNum;  
}

// Function to set the type of the block.
int BlockBuffer::setBlockType(int blockType) {
    unsigned char* bufferPtr;
    int ret = loadBlockAndGetBufferPtr(&bufferPtr);

    if (ret != SUCCESS)
        return ret; // Return error if unable to load block.

    *((int32_t*)bufferPtr) = blockType; // Set the block type in the buffer.

    StaticBuffer::blockAllocMap[this->blockNum] = blockType; // Update the block allocation map.

    return StaticBuffer::setDirtyBit(this->blockNum); // Mark the block as dirty.
}

// Function to find a free block on the disk and initialize it with the provided block type.
int BlockBuffer::getFreeBlock(int blockType) {
    int freeBlock = -1;
    for (int i = 0; i < DISK_BLOCKS; i++) {
        if (StaticBuffer::blockAllocMap[i] == UNUSED_BLK) {
            freeBlock = i;
            break;
        }
    }

    if (freeBlock == -1)
        return E_DISKFULL;

    this->blockNum = freeBlock;

    int freeBuffer = StaticBuffer::getFreeBuffer(freeBlock);

    HeadInfo header;
    header.pblock = -1;
    header.lblock = -1;
    header.rblock = -1;
    header.numEntries = 0;
    header.numAttrs = 0;
    header.numSlots = 0;

    this->setHeader(&header);
    this->setBlockType(blockType);


    return freeBlock;
}