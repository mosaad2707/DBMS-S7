#include "BlockBuffer.h"

#include <cstdlib>
#include <cstring>
// The declarations for these functions can be found in "BlockBuffer.h"
int compareAttrs(union Attribute attr1, union Attribute attr2, int attrType) {
    int diff;
    (attrType == NUMBER)
        ? diff = attr1.nVal - attr2.nVal
        : diff = strcmp(attr1.sVal, attr2.sVal);
    if (diff > 0)
        return 1; // attr1 > attr2
    else if (diff < 0)
        return -1; //attr 1 < attr2
    else 
        return 0;
}
// Constructor for the BlockBuffer class
BlockBuffer::BlockBuffer(int blockNum) {
    // Initialize the member variable blockNum with the argument passed to the constructor
    this->blockNum = blockNum;
}

// Constructor for the RecBuffer class that calls the parent class constructor
RecBuffer::RecBuffer(int blockNum) : BlockBuffer::BlockBuffer(blockNum) {}

// Function to load the block header into the argument pointer
int BlockBuffer::getHeader(struct HeadInfo *head) {
    unsigned char buffer[BLOCK_SIZE];  // Create a buffer to read the block data

    // Read the block at the current block number into the buffer
    Disk::readBlock(buffer, this->blockNum);

    // Populate the fields of the HeadInfo structure from the buffer
    // The offset values (16, 20, 24) are determined by the data structure layout
    memcpy(&head->numSlots, buffer + 24, 4);   // Copy 4 bytes from the buffer to head->numSlots
    memcpy(&head->numEntries, buffer + 16, 4); // Copy 4 bytes from the buffer to head->numEntries
    memcpy(&head->numAttrs, buffer + 20, 4);   // Copy 4 bytes from the buffer to head->numAttrs
    memcpy(&head->lblock, buffer + 8, 4);      // Copy 4 bytes from the buffer to head->lblock
    memcpy(&head->rblock, buffer + 12, 4);     // Copy 4 bytes from the buffer to head->rblock

    return SUCCESS;  // Return SUCCESS to indicate the operation was successful
}

// Function to load the record at a specific slot number into the argument pointer
int RecBuffer::getRecord(union Attribute *rec, int slotNum) {
    struct HeadInfo head;  // Declare a HeadInfo structure to store the header information

    // Retrieve the header information using the getHeader function
    this->getHeader(&head);

    int attrCount = head.numAttrs;  // Number of attributes in the record
    int slotCount = head.numSlots;  // Number of slots in the block

    // Create a buffer to read the block data
    unsigned char buffer[BLOCK_SIZE];
    // Read the block at the current block number into the buffer
    Disk::readBlock(buffer, this->blockNum);

    /* Calculate the offset to the desired record in the buffer
       - HEADER_SIZE is the size of the header in the block
       - slotCount is the number of slots, and each slot occupies 1 byte
       - recordSize is the size of one record, calculated as attrCount * ATTR_SIZE
       - The record at slotNum is located at HEADER_SIZE + slotMapSize + (recordSize * slotNum)
    */
    int recordSize = attrCount * ATTR_SIZE;  // Calculate the size of one record
    unsigned char *slotPointer = buffer + HEADER_SIZE + slotCount + (recordSize * slotNum);

    // Load the record data into the rec data structure
    memcpy(rec, slotPointer, recordSize);

    return SUCCESS;  // Return SUCCESS to indicate the operation was successful
}
//Stage 3 
// int BlockBuffer::loadBlockAndGetBufferPtr(unsigned char **buffPtr) {
//   // check whether the block is already present in the buffer using StaticBuffer.getBufferNum()
//   int bufferNum = StaticBuffer::getBufferNum(this->blockNum);

//   if (bufferNum == E_BLOCKNOTINBUFFER) {
//     bufferNum = StaticBuffer::getFreeBuffer(this->blockNum);

//     if (bufferNum == E_OUTOFBOUND) {
//       return E_OUTOFBOUND;
//     }

//     Disk::readBlock(StaticBuffer::blocks[bufferNum], this->blockNum);
//   }

//   // store the pointer to this buffer (blocks[bufferNum]) in *buffPtr
//   *buffPtr = StaticBuffer::blocks[bufferNum];

//   return SUCCESS;
// }

/* used to get the slotmap from a record block
NOTE: this function expects the caller to allocate memory for `*slotMap`
*/
int RecBuffer::getSlotMap(unsigned char *slotMap) {
  unsigned char *bufferPtr;

  // get the starting address of the buffer containing the block using loadBlockAndGetBufferPtr().
  int ret = loadBlockAndGetBufferPtr(&bufferPtr);
  if (ret != SUCCESS) {
    return ret;
  }

  struct HeadInfo head;
  // get the header of the block using getHeader() function
  ret = getHeader(&head);

  int slotCount = head.numSlots;

  // get a pointer to the beginning of the slotmap in memory by offsetting HEADER_SIZE
  unsigned char *slotMapInBuffer = bufferPtr + HEADER_SIZE;

  // copy the values from `slotMapInBuffer` to `slotMap` (size is `slotCount`)
  memcpy(slotMap, slotMapInBuffer, slotCount);

  return SUCCESS;
}

/* NOTE: This function will NOT check if the block has been initialised as a
   record or an index block. It will copy whatever content is there in that
   disk block to the buffer.
   Also ensure that all the methods accessing and updating the block's data
   should call the loadBlockAndGetBufferPtr() function before the access or
   update is done. This is because the block might not be present in the
   buffer due to LRU buffer replacement. So, it will need to be bought back
   to the buffer before any operations can be done.
 */
int BlockBuffer::loadBlockAndGetBufferPtr(unsigned char ** buffPtr) {
    /* check whether the block is already present in the buffer
       using StaticBuffer.getBufferNum() */
    int bufferNum = StaticBuffer::getBufferNum(this->blockNum);

    // if present (!=E_BLOCKNOTINBUFFER),
        // set the timestamp of the corresponding buffer to 0 and increment the
        // timestamps of all other occupied buffers in BufferMetaInfo.

    // else
        // get a free buffer using StaticBuffer.getFreeBuffer()

        // if the call returns E_OUTOFBOUND, return E_OUTOFBOUND here as
        // the blockNum is invalid

        // Read the block into the free buffer using readBlock()
    if (bufferNum == E_BLOCKNOTINBUFFER) {
        bufferNum = StaticBuffer::getFreeBuffer(this->blockNum);

        if (bufferNum == E_OUTOFBOUND)
            return E_OUTOFBOUND;

        Disk::readBlock(StaticBuffer::blocks[bufferNum], this->blockNum);
    }
    else {
        for (int i = 0; i < BUFFER_CAPACITY; i++) {
            if (!StaticBuffer::metainfo[i].free)
                StaticBuffer::metainfo[i].timeStamp++;
        }

        StaticBuffer::metainfo[bufferNum].timeStamp = 0;
    }
    

    // store the pointer to this buffer (blocks[bufferNum]) in *buffPtr

    // return SUCCESS;

    *buffPtr = StaticBuffer::blocks[bufferNum];

    return SUCCESS;
}

int RecBuffer::setRecord(union Attribute *rec, int slotNum) {
    unsigned char *bufferPtr;

    /* Load the block into the buffer and get the starting address of the buffer
       containing the block using loadBlockAndGetBufferPtr(&bufferPtr). */

    int ret = loadBlockAndGetBufferPtr(&bufferPtr);
    
    // If loading the block fails, return the error code
    if (ret != SUCCESS)
        return ret;

    /* Get the header of the block using the getHeader() function, which contains 
       information like the number of attributes and the number of slots. */

    HeadInfo header;
    this->getHeader(&header);

    // Get the number of attributes in the block
    int numAttrs = header.numAttrs;

    // Get the number of slots in the block
    int numSlots = header.numSlots;

    // If the input slotNum is not within the permitted range, return an out-of-bound error
    if (slotNum < 0 || slotNum >= numSlots)
        return E_OUTOFBOUND;

    /* Calculate the record size based on the number of attributes. Each record will
       be of size ATTR_SIZE * numAttrs. Offset bufferPtr to point to the beginning of 
       the record at the required slot. The block contains the header, the slotmap, 
       followed by all the records. So, for example, the record at slot x will be at 
       bufferPtr + HEADER_SIZE + (x * recordSize). */

    int recordSize = numAttrs * ATTR_SIZE;
    unsigned char* recordPtr = bufferPtr + HEADER_SIZE + numSlots + slotNum * recordSize;

    // Copy the record from `rec` to the buffer using memcpy
    memcpy(recordPtr, rec, recordSize);

    /* Update the dirty bit using setDirtyBit() to indicate that the block has been modified.
       (Note: This function call should not fail since the block is already in the buffer and
       the blockNum is valid. If the call does fail, there exists some other issue in the code.) */

    StaticBuffer::setDirtyBit(this->blockNum);

    // Return SUCCESS to indicate that the operation was successful
    return SUCCESS;
}

//Stage 7
int BlockBuffer::setHeader(struct HeadInfo *head){

    unsigned char *bufferPtr;
    // get the starting address of the buffer containing the block using
    // loadBlockAndGetBufferPtr(&bufferPtr).
    int ret = loadBlockAndGetBufferPtr(&bufferPtr);

    // if loadBlockAndGetBufferPtr(&bufferPtr) != SUCCESS
        // return the value returned by the call.
    if(ret != SUCCESS)
        return ret;

    // cast bufferPtr to type HeadInfo*
    struct HeadInfo *bufferHeader = (struct HeadInfo *)bufferPtr;

    // copy the fields of the HeadInfo pointed to by head (except reserved) to
    // the header of the block (pointed to by bufferHeader)
    //(hint: bufferHeader->numSlots = head->numSlots )
    bufferHeader->numSlots = head->numSlots;
    bufferHeader->numEntries = head->numEntries;
    bufferHeader->numAttrs = head->numAttrs;
    bufferHeader->lblock = head->lblock;
    bufferHeader->rblock = head->rblock;

    // update dirty bit by calling StaticBuffer::setDirtyBit()
    // if setDirtyBit() failed, return the error code
    

    // return SUCCESS;
    return StaticBuffer::setDirtyBit(this->blockNum);
}

int BlockBuffer::setBlockType(int blockType) {
    unsigned char* bufferPtr;

    // Get the starting address of the buffer containing the block
    // using loadBlockAndGetBufferPtr(&bufferPtr).
    int ret = loadBlockAndGetBufferPtr(&bufferPtr);

    // If loading the block fails, return the error code
    if (ret != SUCCESS)
        return ret;

    // Store the input block type in the first 4 bytes of the buffer.
    // (Hint: Cast bufferPtr to int32_t* and then assign it)
    *((int32_t*)bufferPtr) = blockType;

    // Update the StaticBuffer::blockAllocMap entry corresponding to the
    // object's block number to `blockType`.
    StaticBuffer::blockAllocMap[this->blockNum] = blockType;

    // Update the dirty bit by calling StaticBuffer::setDirtyBit()
    // If setDirtyBit() failed, return the returned value from the call
    return StaticBuffer::setDirtyBit(this->blockNum);
}

int BlockBuffer::getFreeBlock(int blockType) {
    int freeBlock = -1;

    // Iterate through the StaticBuffer::blockAllocMap to find the block number of a free block on the disk.
    for (int i = 0; i < DISK_BLOCKS; i++) {
        if (StaticBuffer::blockAllocMap[i] == UNUSED_BLK) {  // Check if the block is unused
            freeBlock = i;  // Assign the block number of the free block
            break;  // Exit the loop as soon as a free block is found
        }
    }

    // If no block is free, return E_DISKFULL.
    if (freeBlock == -1)
        return E_DISKFULL;

    // Set the object's blockNum to the block number of the free block.
    this->blockNum = freeBlock;

    // Find a free buffer using StaticBuffer::getFreeBuffer().
    int freeBuffer = StaticBuffer::getFreeBuffer(freeBlock);

    // Initialize the header of the block by passing a struct HeadInfo with default values
    // to the setHeader() function.
    HeadInfo header;
    header.pblock = -1;   // Previous block pointer is set to -1 (indicating no previous block)
    header.lblock = -1;   // Left block pointer is set to -1 (indicating no left block)
    header.rblock = -1;   // Right block pointer is set to -1 (indicating no right block)
    header.numEntries = 0; // Number of entries in the block is set to 0
    header.numAttrs = 0;   // Number of attributes in the block is set to 0
    header.numSlots = 0;   // Number of slots in the block is set to 0

    // Set the header of the block
    this->setHeader(&header);

    // Update the block type of the block to the input block type using setBlockType().
    this->setBlockType(blockType);

    // Return the block number of the free block.
    return freeBlock;
}

BlockBuffer::BlockBuffer(char blockType) {
    int BlockTypeNumber;

    // Determine the block type number based on the input character
    if (blockType == 'R')
        BlockTypeNumber = REC;           // Set BlockTypeNumber to REC for 'R' (Record block)
    else if (blockType == 'I')
        BlockTypeNumber = IND_INTERNAL;  // Set BlockTypeNumber to IND_INTERNAL for 'I' (Internal Index block)
    else if (blockType == 'L')
        BlockTypeNumber = IND_LEAF;      // Set BlockTypeNumber to IND_LEAF for 'L' (Leaf Index block)
    else 
        BlockTypeNumber = UNUSED_BLK;    // Set BlockTypeNumber to UNUSED_BLK for any other value

    // Allocate a block on the disk and a buffer in memory to hold the new block of
    // the given type using getFreeBlock function and get the return error codes if any.
    int blockNum = getFreeBlock(BlockTypeNumber);

    // Set the blockNum field of the object to the allocated block number
    // if the method returned a valid block number
    this->blockNum = blockNum;

    // If the block number is invalid (less than 0 or greater than or equal to DISK_BLOCKS),
    // the constructor exits without further action.
    if (blockNum < 0 || blockNum >= DISK_BLOCKS)
        return;
}


RecBuffer::RecBuffer() : BlockBuffer::BlockBuffer('R') {}
// call parent non-default constructor with 'R' denoting record block.

int RecBuffer::setSlotMap(unsigned char* slotMap) {
    unsigned char* bufferPtr;

    // Get the starting address of the buffer containing the block using loadBlockAndGetBufferPtr(&bufferPtr).
    int ret = loadBlockAndGetBufferPtr(&bufferPtr);

    // If loadBlockAndGetBufferPtr(&bufferPtr) fails, return the value returned by the call.
    if (ret != SUCCESS)
        return ret;

    // Get the header of the block using the getHeader() function.
    struct HeadInfo head;
    getHeader(&head);

    // Retrieve the number of slots in the block from the header.
    int slotCount = head.numSlots;

    // The slot map starts at bufferPtr + HEADER_SIZE. Copy the contents of the argument `slotMap` 
    // to the buffer, replacing the existing slot map. The size of the slot map is `slotCount`.
    unsigned char* slotMapInBuffer = bufferPtr + HEADER_SIZE;
    memcpy(slotMapInBuffer, slotMap, slotCount);

    // Update the dirty bit using StaticBuffer::setDirtyBit. If setDirtyBit fails, return the error code.
    return StaticBuffer::setDirtyBit(this->blockNum);
}

int BlockBuffer::getBlockNum(){

    //return corresponding block number.
    return this->blockNum;
}


