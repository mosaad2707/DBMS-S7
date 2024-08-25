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
