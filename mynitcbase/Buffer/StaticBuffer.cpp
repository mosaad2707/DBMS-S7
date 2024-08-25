#include "StaticBuffer.h"


// the declarations for this class can be found at "StaticBuffer.h"

unsigned char StaticBuffer::blocks[BUFFER_CAPACITY][BLOCK_SIZE];
struct BufferMetaInfo StaticBuffer::metainfo[BUFFER_CAPACITY];


StaticBuffer::StaticBuffer() {
    for (int i = 0; i < BUFFER_CAPACITY; i++) {
        metainfo[i].free = true;
        metainfo[i].dirty = false;
        metainfo[i].timeStamp = -1;
        metainfo[i].blockNum = -1;
    }
}

StaticBuffer::~StaticBuffer() {
    for (int i = 0; i < BUFFER_CAPACITY; i++) {
        if (!metainfo[i].free && metainfo[i].dirty) 
            Disk::writeBlock(StaticBuffer::blocks[i], metainfo[i].blockNum);
    }
}

int StaticBuffer::getFreeBuffer(int blockNum) {
    // Check if the block number is within valid bounds
    if (blockNum < 0 || blockNum >= DISK_BLOCKS)
        return E_OUTOFBOUND; // Return an error if the block number is out of bounds

    // Increment the time stamp for all occupied buffers
    for (int i = 0; i < BUFFER_CAPACITY; i++) {
        if (!metainfo[i].free)
            metainfo[i].timeStamp++; // Increment the time stamp for buffers that are in use
    }

    int allocatedBuffer = -1; // Initialize variable to store the index of the allocated buffer

    // Search for a free buffer in the buffer pool
    for (int i = 0; i < BUFFER_CAPACITY; i++) {
        if (metainfo[i].free) { // If a free buffer is found
            allocatedBuffer = i; // Set the index of the free buffer
            break; // Exit the loop as we've found a free buffer
        }
    }

    // If no free buffer was found, perform a replacement strategy
    if (allocatedBuffer == -1) {
        int highestTimeStamp = 0; // Variable to track the buffer with the highest time stamp
        for (int i = 0; i < BUFFER_CAPACITY; i++) {
            // Find the buffer that has been in use the longest (highest time stamp)
            if (metainfo[i].timeStamp > highestTimeStamp) {
                highestTimeStamp = metainfo[i].timeStamp;
                allocatedBuffer = i; // Set the index of the buffer to be replaced
            }
        }

        // If the selected buffer is dirty (modified), write it back to disk
        if (metainfo[allocatedBuffer].dirty)
            Disk::writeBlock(StaticBuffer::blocks[allocatedBuffer], metainfo[allocatedBuffer].blockNum);
    }

    // Update the metadata for the allocated buffer
    metainfo[allocatedBuffer].free = false;         // Mark the buffer as occupied
    metainfo[allocatedBuffer].dirty = false;        // Reset the dirty flag
    metainfo[allocatedBuffer].blockNum = blockNum;  // Set the block number for the buffer
    metainfo[allocatedBuffer].timeStamp = 0;        // Reset the time stamp for the buffer

    return allocatedBuffer; // Return the index of the allocated buffer
}

/* Get the buffer index where a particular block is stored
   or E_BLOCKNOTINBUFFER otherwise
*/
int StaticBuffer::getBufferNum(int blockNum) {
  // Check if blockNum is valid (between zero and DISK_BLOCKS)
  // and return E_OUTOFBOUND if not valid.
     if (blockNum < 0 || blockNum > DISK_BLOCKS) {
        return E_OUTOFBOUND;
    }
  // find and return the bufferIndex which corresponds to blockNum (check metainfo)
    for (int bufferIndex=0 ; bufferIndex<BUFFER_CAPACITY ; bufferIndex++) {
        if (metainfo[bufferIndex].blockNum == blockNum) {
        return bufferIndex;
        }
    }

  // if block is not in the buffer
  return E_BLOCKNOTINBUFFER;
}

int StaticBuffer::setDirtyBit(int blockNum){
    // find the buffer index corresponding to the block using getBufferNum().
    int bufferNum = getBufferNum(blockNum);

    // if block is not present in the buffer (bufferNum = E_BLOCKNOTINBUFFER)
    //     return E_BLOCKNOTINBUFFER
    if (bufferNum == E_BLOCKNOTINBUFFER) {
        return E_BLOCKNOTINBUFFER;
    }

    // if blockNum is out of bound (bufferNum = E_OUTOFBOUND)
    //     return E_OUTOFBOUND
    if (bufferNum == E_OUTOFBOUND) {
        return E_OUTOFBOUND;
    }

    // else
    //     (the bufferNum is valid)
    //     set the dirty bit of that buffer to true in metainfo
    metainfo[bufferNum].dirty = true;

    // return SUCCESS
    return SUCCESS;
}