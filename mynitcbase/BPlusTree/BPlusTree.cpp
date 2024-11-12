#include "BPlusTree.h"
#include <stdio.h>
#include <cstring>

RecId BPlusTree::bPlusSearch(int relId, char attrName[ATTR_SIZE], Attribute attrVal, int op) {
    // declare searchIndex which will be used to store search index for attrName.
    IndexId searchIndex;

    /* get the search index corresponding to attribute with name attrName
       using AttrCacheTable::getSearchIndex(). */
    AttrCacheTable::getSearchIndex(relId, attrName, &searchIndex);   

    AttrCatEntry attrCatEntry;
    /* load the attribute cache entry into attrCatEntry using
     AttrCacheTable::getAttrCatEntry(). */
    AttrCacheTable::getAttrCatEntry(relId, attrName, &attrCatEntry); 

    // declare variables block and index which will be used during search
    int block, index;

    if (searchIndex.block == -1 && searchIndex.index == -1) {
        // (search is done for the first time)

        // start the search from the first entry of root.
        block = attrCatEntry.rootBlock;
        index = 0;

        if (block==-1) {
            return RecId{-1, -1};
        }

    } 

    else {
        /*a valid searchIndex points to an entry in the leaf index of the attribute's
        B+ Tree which had previously satisfied the op for the given attrVal.*/

        block = searchIndex.block;
        index = searchIndex.index + 1;  // search is resumed from the next index.

        // load block into leaf using IndLeaf::IndLeaf().
        IndLeaf leaf(block);

        // declare leafHead which will be used to hold the header of leaf.
        HeadInfo leafHead;

        // load header into leafHead using BlockBuffer::getHeader().
        leaf.getHeader(&leafHead);

        if (index >= leafHead.numEntries) {
            /* (all the entries in the block has been searched; search from the
            beginning of the next leaf index block. */
            block=leafHead.rblock;
            index=0;

            // update block to rblock of current block and index to 0.

            if (block == -1) {
                // (end of linked list reached - the search is done.)
                return RecId{-1, -1};
            }
        }
    }

    /******  Traverse through all the internal nodes according to value
             of attrVal and the operator op                             ******/

    /* (This section is only needed when
        - search restarts from the root block (when searchIndex is reset by caller)
        - root is not a leaf
        If there was a valid search index, then we are already at a leaf block
        and the test condition in the following loop will fail)
    */

    while(StaticBuffer::getStaticBlockType(block)==IND_INTERNAL) {  //use StaticBuffer::getStaticBlockType()

        // load the block into internalBlk using IndInternal::IndInternal().
        IndInternal internalBlk(block);

        HeadInfo intHead;

        // load the header of internalBlk into intHead using BlockBuffer::getHeader()
        internalBlk.getHeader(&intHead);

        // declare intEntry which will be used to store an entry of internalBlk.
        InternalEntry intEntry;

        if (op == NE || op == LT || op == LE) {
            /*
            - NE: need to search the entire linked list of leaf indices of the B+ Tree,
            starting from the leftmost leaf index. Thus, always move to the left.


            - LT and LE: the attribute values are arranged in ascending order in the
            leaf indices of the B+ Tree. Values that satisfy these conditions, if
            any exist, will always be found in the left-most leaf index. Thus,
            always move to the left.
            */
           internalBlk.getEntry(&intEntry, 0);

            // load entry in the first slot of the block into intEntry
            // using IndInternal::getEntry().

            block = intEntry.lChild;

        } else {
            /*
            - EQ, GT and GE: move to the left child of the first entry that is
            greater than (or equal to) attrVal
            (we are trying to find the first entry that satisfies the condition.
            since the values are in ascending order we move to the left child which
            might contain more entries that satisfy the condition)
            */
           int final =-1;

            /*
             traverse through all entries of internalBlk and find an entry that
             satisfies the condition.
             if op == EQ or GE, then intEntry.attrVal >= attrVal
             if op == GT, then intEntry.attrVal > attrVal
             Hint: the helper function compareAttrs() can be used for comparing
            */
            for (int i = 0; i < intHead.numEntries; i++) {
                internalBlk.getEntry(&intEntry, i);

                int cmpVal = compareAttrs(intEntry.attrVal, attrVal, attrCatEntry.attrType);

                if (cmpVal>=0) {
                    final = i;
                    break;
                }
            }

            if (final == -1) {
                // move to the left child of that entry
                internalBlk.getEntry(&intEntry, intHead.numEntries-1);
                block = intEntry.rChild;

            } else {
                // move to the right child of the last entry of the block
                // i.e numEntries - 1 th entry of the block
                internalBlk.getEntry(&intEntry, final);
                block = intEntry.lChild;
            }
        }
    }

    // NOTE: `block` now has the block number of a leaf index block.

    /******  Identify the first leaf index entry from the current position
                that satisfies our condition (moving right)             ******/

    while (block != -1) {
        // load the block into leafBlk using IndLeaf::IndLeaf().
        IndLeaf leafBlk(block);
        HeadInfo leafHead;

        // load the header to leafHead using BlockBuffer::getHeader().
        leafBlk.getHeader(&leafHead);

        // declare leafEntry which will be used to store an entry from leafBlk
        Index leafEntry;

        while (index < leafHead.numEntries) {

            // load entry corresponding to block and index into leafEntry
            // using IndLeaf::getEntry().
            leafBlk.getEntry(&leafEntry, index);

            int cmpVal = compareAttrs(leafEntry.attrVal, attrVal, attrCatEntry.attrType);

            if (
                (op == EQ && cmpVal == 0) ||
                (op == LE && cmpVal <= 0) ||
                (op == LT && cmpVal < 0) ||
                (op == GT && cmpVal > 0) ||
                (op == GE && cmpVal >= 0) ||
                (op == NE && cmpVal != 0)
            ) {
                // (entry satisfying the condition found)
                

                // set search index to {block, index}
                searchIndex = {block, index};

                // return the recId {leafEntry.block, leafEntry.slot}.
                AttrCacheTable::setSearchIndex(relId, attrName, &searchIndex);
                return RecId{leafEntry.block, leafEntry.slot};

            } else if ((op == EQ || op == LE || op == LT) && cmpVal > 0) {
                /*future entries will not satisfy EQ, LE, LT since the values
                    are arranged in ascending order in the leaves */
                    return RecId({-1, -1});

                // return RecId {-1, -1};
            }

            // search next index.
            ++index;
        }

        /*only for NE operation do we have to check the entire linked list;
        for all the other op it is guaranteed that the block being searched
        will have an entry, if it exists, satisying that op. */
        if (op != NE) {
            break;
        }

        // block = next block in the linked list, i.e., the rblock in leafHead.
        // update index to 0.
        block = leafHead.rblock;
        index = 0;
    }

    // no entry satisying the op was found; return the recId {-1,-1}
    return RecId{-1, -1};
}

int BPlusTree::bPlusCreate(int relId, char attrName[ATTR_SIZE]) {
    // if relId is either RELCAT_RELID or ATTRCAT_RELID:
    //     return E_NOTPERMITTED.
    if (relId == RELCAT_RELID || relId == ATTRCAT_RELID)
        return E_NOTPERMITTED;

    // get the attribute catalog entry of attribute `attrName`
    // using AttrCacheTable::getAttrCatEntry()
    AttrCatEntry attrCatEntry;
    int retVal = AttrCacheTable::getAttrCatEntry(relId, attrName, &attrCatEntry);

    // if getAttrCatEntry fails
    //     return the error code from getAttrCatEntry
    if (retVal != SUCCESS)
        return retVal;

    // if an index already exists for the attribute (check rootBlock field)
    if (attrCatEntry.rootBlock != -1)
        return SUCCESS;

    /******Creating a new B+ Tree ******/

    // get a free leaf block using constructor 1 to allocate a new block
    IndLeaf rootBlockBuffer;

    // declare rootBlock to store the blockNumber of the new leaf block
    int rootBlock = rootBlockBuffer.getBlockNum();

    // if there is no more disk space for creating an index
    if (rootBlock == E_DISKFULL)
        return E_DISKFULL;

    // update rootBlock in attribute catalog entry
    attrCatEntry.rootBlock = rootBlock;
    AttrCacheTable::setAttrCatEntry(relId, attrName, &attrCatEntry);

    RelCatEntry relCatEntry;

    // load the relation catalog entry into relCatEntry
    // using RelCacheTable::getRelCatEntry().
    retVal = RelCacheTable::getRelCatEntry(relId, &relCatEntry);
    if (retVal != SUCCESS)
        return retVal;

    int currentBlock = relCatEntry.firstBlk;

    /***** Traverse all the blocks in the relation and insert them one
           by one into the B+ Tree *****/
    while (currentBlock != -1) {
        // declare a RecBuffer object for `currentBlock`
        RecBuffer blockBuffer(currentBlock);

        // load the slot map into slotMap using RecBuffer::getSlotMap().
        unsigned char slotMap[relCatEntry.numSlotsPerBlk];
        blockBuffer.getSlotMap(slotMap);

        // for every occupied slot of the block
        for (int slotNum = 0; slotNum < relCatEntry.numSlotsPerBlk; slotNum++) {
            if (slotMap[slotNum] == SLOT_UNOCCUPIED)
                continue;

            // load the record corresponding to the slot into `record`
            // using RecBuffer::getRecord().
            Attribute record[relCatEntry.numAttrs];
            blockBuffer.getRecord(record, slotNum);

            // declare recId and store the rec-id of this record in it
            RecId recId = {currentBlock, slotNum};

            // insert the attribute value corresponding to attrName from the record
            // into the B+ tree using bPlusInsert.
            retVal = BPlusTree::bPlusInsert(relId, attrName, record[attrCatEntry.offset], recId);

            // if insert fails due to disk being full, return E_DISKFULL
            if (retVal == E_DISKFULL)
                return E_DISKFULL;
        }

        // get the header of the block using BlockBuffer::getHeader()
        HeadInfo headerInfo;
        blockBuffer.getHeader(&headerInfo);

        // set currentBlock = rblock of current block (from the header)
        currentBlock = headerInfo.rblock;
    }

    return SUCCESS;
}

int BPlusTree::bPlusDestroy(int rootBlockNum) {
    // if rootBlockNum lies outside the valid range [0,DISK_BLOCKS-1]
    //     return E_OUTOFBOUND.
    if (rootBlockNum < 0 || rootBlockNum >= DISK_BLOCKS)
        return E_OUTOFBOUND;

    // get the type of block (using StaticBuffer::getStaticBlockType()).
    int blockType = StaticBuffer::getStaticBlockType(rootBlockNum);

    if (blockType == IND_LEAF) {
        // declare an instance of IndLeaf for rootBlockNum using appropriate constructor
        IndLeaf leafNode(rootBlockNum);

        // release the block using BlockBuffer::releaseBlock().
        leafNode.releaseBlock();
        return SUCCESS;

    } else if (blockType == IND_INTERNAL) {
        // declare an instance of IndInternal for rootBlockNum using appropriate constructor
        IndInternal internalNode(rootBlockNum);

        // load the header of the block using BlockBuffer::getHeader().
        HeadInfo internalHeader;
        internalNode.getHeader(&internalHeader);

        // iterate through all the entries of the internalBlk
        // and destroy the lChild of the first entry and rChild of all entries
        InternalEntry internalEntry;
        internalNode.getEntry(&internalEntry, 0);

        // Destroy the left child of the first entry
        if (internalEntry.lChild != -1) {
            int retVal = bPlusDestroy(internalEntry.lChild);
            if (retVal != SUCCESS)
                return retVal;
        }

        // Traverse each entry and destroy the right child
        int numEntries = internalHeader.numEntries;
        for (int entryIndex = 0; entryIndex < numEntries; entryIndex++) {
            internalNode.getEntry(&internalEntry, entryIndex);

            if (internalEntry.rChild != -1) {
                int retVal = bPlusDestroy(internalEntry.rChild);
                if (retVal != SUCCESS)
                    return retVal;
            }
        }

        // release the block using BlockBuffer::releaseBlock().
        internalNode.releaseBlock();
        return SUCCESS;

    } else {
        // (block is not an index block.)
        return E_INVALIDBLOCK;
    }
}

int BPlusTree::bPlusInsert(int relId, char attrName[ATTR_SIZE], Attribute attrVal, RecId recId) {
    // get the attribute cache entry corresponding to attrName
    // using AttrCacheTable::getAttrCatEntry().
    AttrCatEntry attrCatEntry;
    int retVal = AttrCacheTable::getAttrCatEntry(relId, attrName, &attrCatEntry);

    // if getAttrCatEntry() failed
    //     return the error code
    if (retVal != SUCCESS)
        return retVal;

    // rootBlock of B+ Tree (from attrCatEntry)
    int rootBlockNum = attrCatEntry.rootBlock;

    // if there is no index on attribute (rootBlock is -1)
    if (rootBlockNum == -1)
        return E_NOINDEX;

    // find the leaf block to which insertion is to be done using the
    // findLeafToInsert() function
    int leafBlockNum = findLeafToInsert(rootBlockNum, attrVal, attrCatEntry.attrType);

    // insert the attrVal and recId to the leaf block at blockNum using the
    // insertIntoLeaf() function.
    // declare a struct Index with attrVal = attrVal, block = recId.block and
    // slot = recId.slot to pass as argument to the function.
    Index entry;
    entry.attrVal = attrVal;
    entry.block = recId.block;
    entry.slot = recId.slot;

    // insertIntoLeaf(relId, attrName, leafBlockNum, Index entry)
    // NOTE: the insertIntoLeaf() function will propagate the insertion to the
    //       required internal nodes by calling the required helper functions
    //       like insertIntoInternal() or createNewRoot()
    retVal = insertIntoLeaf(relId, attrName, leafBlockNum, entry);

    // if insertIntoLeaf() returns E_DISKFULL
    if (retVal == E_DISKFULL) {
        // destroy the existing B+ tree by passing the rootBlock to bPlusDestroy().
        BPlusTree::bPlusDestroy(rootBlockNum);

        // update the rootBlock of attribute catalog cache entry to -1 using
        // AttrCacheTable::setAttrCatEntry().
        attrCatEntry.rootBlock = -1;
        AttrCacheTable::setAttrCatEntry(relId, attrName, &attrCatEntry);

        return E_DISKFULL;
    }

    return SUCCESS;
}

int BPlusTree::findLeafToInsert(int rootBlock, Attribute attrVal, int attrType) {
    int blockNum = rootBlock;

    // Traverse down the B+ Tree until a leaf node is reached
    while (StaticBuffer::getStaticBlockType(blockNum) != IND_LEAF) {

        // declare an IndInternal object for block using appropriate constructor
        IndInternal internalBlock(blockNum);

        // get header of the block using BlockBuffer::getHeader()
        HeadInfo internalHeader;
        internalBlock.getHeader(&internalHeader);

        int numEntries = internalHeader.numEntries;
        InternalEntry internalEntry;

        // iterate through all the entries to find the first entry
        // whose attribute value >= value to be inserted.
        int targetIndex = -1;
        for (int i = 0; i < numEntries; i++) {
            internalBlock.getEntry(&internalEntry, i);

            // Use compareAttrs() to compare values
            if (compareAttrs(internalEntry.attrVal, attrVal, attrType) > 0) {
                targetIndex = i;
                break;
            }
        }

        if (targetIndex == -1) {
            // If no such entry is found, set blockNum to rChild of the last entry
            internalBlock.getEntry(&internalEntry, numEntries - 1);
            blockNum = internalEntry.rChild;
        } else {
            // set blockNum to lChild of the found entry
            internalBlock.getEntry(&internalEntry, targetIndex);
            blockNum = internalEntry.lChild;
        }
    }

    // Return the leaf block number where insertion is to be done
    return blockNum;
}

int BPlusTree::insertIntoLeaf(int relId, char attrName[ATTR_SIZE], int blockNum, Index indexEntry) {
    // Get the attribute cache entry corresponding to attrName
    AttrCatEntry attrCatEntry;
    AttrCacheTable::getAttrCatEntry(relId, attrName, &attrCatEntry);

    // Declare an IndLeaf instance for the block using appropriate constructor
    IndLeaf leafBlock(blockNum);

    // Store the header of the leaf index block into leafHeader
    HeadInfo leafHeader;
    leafBlock.getHeader(&leafHeader);

    int numEntries = leafHeader.numEntries;
    
    // This variable will be used to store a list of index entries with existing indices + the new index to insert
    Index indices[numEntries + 1];

    // Iterate through all the entries in the block and copy them to the array indices.
    // Also insert `indexEntry` at the appropriate position in the indices array, maintaining the ascending order.
    int targetIndex = numEntries;
    Index leafEntry;
    for (int i = 0; i < numEntries; i++) {
        leafBlock.getEntry(&leafEntry, i);
        if (compareAttrs(leafEntry.attrVal, indexEntry.attrVal, attrCatEntry.attrType) > 0) {
            targetIndex = i;
            break;
        }
    }

    // Populate the indices array, including the new entry at the correct position
    for (int i = 0; i < targetIndex; i++)
        leafBlock.getEntry(&indices[i], i);

    indices[targetIndex] = indexEntry;

    for (int i = targetIndex; i < numEntries; i++)
        leafBlock.getEntry(&indices[i + 1], i);

    if (numEntries != MAX_KEYS_LEAF) {
        // If the leaf block has not reached the max limit, increment numEntries and update the header
        leafHeader.numEntries++;
        leafBlock.setHeader(&leafHeader);

        // Populate the entries of the block with the updated indices array
        for (int i = 0; i < leafHeader.numEntries; i++)
            leafBlock.setEntry(&indices[i], i);

        return SUCCESS;
    }

    // If reached here, the indices array has more entries than can fit in a single leaf block, so split the leaf
    int newRightBlock = splitLeaf(blockNum, indices);

    // if splitLeaf() returned E_DISKFULL, return E_DISKFULL
    if (newRightBlock == E_DISKFULL)
        return newRightBlock;

    if (leafHeader.pblock != -1) {
        // If the current leaf block was not the root, insert the middle value into the parent block
        // (i.e., the last value of the left block)
        InternalEntry intEntry;
        intEntry.attrVal = indices[MIDDLE_INDEX_LEAF].attrVal;
        intEntry.lChild = blockNum;
        intEntry.rChild = newRightBlock;

        return insertIntoInternal(relId, attrName, leafHeader.pblock, intEntry);
    }
    else {
        // If the current block was the root block and is now split, create a new root
        return createNewRoot(relId, attrName, indices[MIDDLE_INDEX_LEAF].attrVal, blockNum, newRightBlock);
    }

    return SUCCESS;
}

int BPlusTree::splitLeaf(int leafBlockNum, Index indices[]) {
    // declare rightBlk, an instance of IndLeaf using constructor 1 to obtain new
    // leaf index block that will be used as the right block in the splitting
    IndLeaf rightBlock;

    // declare leftBlk, an instance of IndLeaf using constructor 2 to read from
    // the existing leaf block
    IndLeaf leftBlock(leafBlockNum);

    int leftBlockNum = leafBlockNum;
    int rightBlockNum = rightBlock.getBlockNum();

    // if newly allocated block has blockNum E_DISKFULL
    if (rightBlockNum == E_DISKFULL)
        return E_DISKFULL;

    // get the headers of left block and right block
    HeadInfo leftBlockHeader, rightBlockHeader;
    rightBlock.getHeader(&rightBlockHeader);
    leftBlock.getHeader(&leftBlockHeader);

    // set rightBlkHeader with specified values
    // - number of entries = (MAX_KEYS_LEAF+1)/2 = 32
    // - pblock = pblock of leftBlk
    // - lblock = leftBlkNum
    // - rblock = rblock of leftBlk
    rightBlockHeader.numEntries = (MAX_KEYS_LEAF + 1) / 2;
    rightBlockHeader.pblock = leftBlockHeader.pblock;
    rightBlockHeader.lblock = leftBlockNum;
    rightBlockHeader.rblock = leftBlockHeader.rblock;
    rightBlock.setHeader(&rightBlockHeader);

    // set leftBlkHeader with specified values
    // - number of entries = (MAX_KEYS_LEAF+1)/2 = 32
    // - rblock = rightBlkNum
    leftBlockHeader.numEntries = (MAX_KEYS_LEAF + 1) / 2;
    leftBlockHeader.rblock = rightBlockNum;
    leftBlock.setHeader(&leftBlockHeader);

    // set the first 32 entries of leftBlk = the first 32 entries of indices array
    // and set the first 32 entries of newRightBlk = the next 32 entries of
    // indices array using IndLeaf::setEntry()
    for (int i = 0; i < 32; i++)
        leftBlock.setEntry(&indices[i], i);

    for (int i = 32; i < 64; i++)
        rightBlock.setEntry(&indices[i], i - 32);

    return rightBlockNum;
}

int BPlusTree::insertIntoInternal(int relId, char attrName[ATTR_SIZE], int intBlockNum, InternalEntry intEntry) {
    // Get the attribute cache entry corresponding to attrName
    AttrCatEntry attrCatEntry;
    int ret = AttrCacheTable::getAttrCatEntry(relId, attrName, &attrCatEntry);
    if (ret != SUCCESS)
        return ret;

    // Declare intBlk, an instance of IndInternal for the block corresponding to intBlockNum
    IndInternal intBlock(intBlockNum);

    // Load blockHeader with the header of intBlk
    HeadInfo intHeader;
    intBlock.getHeader(&intHeader);

    // Declare internalEntries to store all existing entries + the new entry
    InternalEntry intEntries[intHeader.numEntries + 1];

    // Iterate through all the entries in the block and copy them to the array
    // `internalEntries`. Insert `indexEntry` at the appropriate position in the
    // array, maintaining the ascending order.
    int targetIndex = intHeader.numEntries;
    InternalEntry entryBuffer;
    for (int i = 0; i < intHeader.numEntries; i++) {
        intBlock.getEntry(&entryBuffer, i);
        if (compareAttrs(entryBuffer.attrVal, intEntry.attrVal, attrCatEntry.attrType) > 0) {
            targetIndex = i;
            break;
        }
    }

    // Populate the entries in internalEntries array, including the new entry
    for (int i = 0; i < targetIndex; i++)
        intBlock.getEntry(&intEntries[i], i);

    intEntries[targetIndex] = intEntry;

    for (int i = targetIndex; i < intHeader.numEntries; i++)
        intBlock.getEntry(&intEntries[i + 1], i);

    // Update the lChild of the entry following the newly added entry
    if (targetIndex < intHeader.numEntries)
        intEntries[targetIndex + 1].lChild = intEntries[targetIndex].rChild;

    if (intHeader.numEntries != MAX_KEYS_INTERNAL) {
        // If internal index block has not reached max limit
        intHeader.numEntries++;
        intBlock.setHeader(&intHeader);

        // Populate the entries of intBlk with internalEntries array
        for (int i = 0; i < intHeader.numEntries; i++)
            intBlock.setEntry(&intEntries[i], i);

        return SUCCESS;
    }

    // If reached here, the internalEntries array has more entries than can fit in a single internal index block
    int newRightBlock = splitInternal(intBlockNum, intEntries);
    if (newRightBlock == E_DISKFULL)
        return E_DISKFULL;

    if (intHeader.pblock != -1) {
        // If the current block was not the root, insert the middle value from internalEntries into the parent block
        InternalEntry entryInParent;
        entryInParent.attrVal = intEntries[MIDDLE_INDEX_INTERNAL].attrVal;
        entryInParent.lChild = intBlockNum;
        entryInParent.rChild = newRightBlock;

        return insertIntoInternal(relId, attrName, intHeader.pblock, entryInParent);
    } else {
        // If the current block was the root block and is now split, create a new root
        return createNewRoot(relId, attrName, intEntries[MIDDLE_INDEX_INTERNAL].attrVal, intBlockNum, newRightBlock);
    }

    return SUCCESS;
}

int BPlusTree::splitInternal(int intBlockNum, InternalEntry internalEntries[]) {
    // Declare rightBlk, an instance of IndInternal to obtain a new internal index block
    IndInternal rightBlock;

    // Declare leftBlk, an instance of IndInternal to read from the existing internal index block
    IndInternal leftBlock(intBlockNum);

    int rightBlockNum = rightBlock.getBlockNum();
    int leftBlockNum = intBlockNum;

    // If newly allocated block has blockNum E_DISKFULL
    if (rightBlockNum == E_DISKFULL)
        return E_DISKFULL;

    // Get the headers of left block and right block
    HeadInfo leftBlockHeader, rightBlockHeader;
    leftBlock.getHeader(&leftBlockHeader);
    rightBlock.getHeader(&rightBlockHeader);

    // Set rightBlkHeader with specified values
    // - number of entries = (MAX_KEYS_INTERNAL) / 2 = 50
    // - pblock = pblock of leftBlk
    rightBlockHeader.numEntries = (MAX_KEYS_INTERNAL / 2);
    rightBlockHeader.pblock = leftBlockHeader.pblock;
    rightBlock.setHeader(&rightBlockHeader);

    // Set leftBlkHeader with specified values
    // - number of entries = (MAX_KEYS_INTERNAL) / 2 = 50
    leftBlockHeader.numEntries = (MAX_KEYS_INTERNAL / 2);
    leftBlock.setHeader(&leftBlockHeader);

    /*
    - Set the first 50 entries of leftBlk = index 0 to 49 of internalEntries array
    - Set the first 50 entries of newRightBlk = entries from index 51 to 100
      of internalEntries array using IndInternal::setEntry().
      (index 50 will be moving to the parent internal index block)
    */
    for (int i = 0; i < MIDDLE_INDEX_INTERNAL; i++)
        leftBlock.setEntry(&internalEntries[i], i);

    for (int i = MIDDLE_INDEX_INTERNAL + 1; i <= 100; i++)
        rightBlock.setEntry(&internalEntries[i], i - MIDDLE_INDEX_INTERNAL - 1);

    // Adjust pblock references for children in the right block
    InternalEntry entryBuffer;
    rightBlock.getEntry(&entryBuffer, 0);
    BlockBuffer childBuffer(entryBuffer.lChild);
    HeadInfo childHeader;
    childBuffer.getHeader(&childHeader);
    childHeader.pblock = rightBlockNum;
    childBuffer.setHeader(&childHeader);

    for (int i = 0; i < rightBlockHeader.numEntries; i++) {
        rightBlock.getEntry(&entryBuffer, i);
        BlockBuffer childBuffer(entryBuffer.rChild);
        childBuffer.getHeader(&childHeader);
        childHeader.pblock = rightBlockNum;
        childBuffer.setHeader(&childHeader);
    }

    return rightBlockNum;
}

int BPlusTree::createNewRoot(int relId, char attrName[ATTR_SIZE], Attribute attrVal, int lChild, int rChild) {
    // Get the attribute cache entry corresponding to attrName
    AttrCatEntry attrCatEntry;
    AttrCacheTable::getAttrCatEntry(relId, attrName, &attrCatEntry);

    // Declare newRootBlk, an instance of IndInternal to allocate a new internal index block on the disk
    IndInternal newRootBlock;
    int newRootBlockNum = newRootBlock.getBlockNum();

    // If unable to allocate new root block due to disk space
    if (newRootBlockNum == E_DISKFULL) {
        // Destroy the right subtree rooted at rChild to free resources
        BPlusTree::bPlusDestroy(lChild);
        BPlusTree::bPlusDestroy(rChild);
        return E_DISKFULL;
    }

    // Update the header of the new block with numEntries = 1
    HeadInfo newRootHeader;
    newRootBlock.getHeader(&newRootHeader);
    newRootHeader.numEntries = 1;
    newRootBlock.setHeader(&newRootHeader);

    // Create an InternalEntry with lChild, attrVal, and rChild and set it as the first entry in newRootBlk
    InternalEntry intEntry;
    intEntry.attrVal = attrVal;
    intEntry.lChild = lChild;
    intEntry.rChild = rChild;
    newRootBlock.setEntry(&intEntry, 0);

    // Update the parent pointers (pblock) of lChild and rChild to newRootBlockNum
    BlockBuffer leftChild(lChild), rightChild(rChild);
    HeadInfo leftHeader, rightHeader;

    leftChild.getHeader(&leftHeader);
    leftHeader.pblock = newRootBlockNum;
    leftChild.setHeader(&leftHeader);

    rightChild.getHeader(&rightHeader);
    rightHeader.pblock = newRootBlockNum;
    rightChild.setHeader(&rightHeader);

    // Update rootBlock to newRootBlockNum in the attribute cache entry for attrName
    attrCatEntry.rootBlock = newRootBlockNum;
    AttrCacheTable::setAttrCatEntry(relId, attrName, &attrCatEntry);

    return SUCCESS;
}