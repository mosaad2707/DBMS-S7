#include "BlockAccess.h"
#include <cstring>

RecId BlockAccess::linearSearch(int relId, char attrName[ATTR_SIZE], union Attribute attrVal, int op) {
    // Initialize a RecId object to store the previous record ID
    RecId prevRecId;

    // Retrieve the last search index (block and slot) for the given relation ID
    RelCacheTable::getSearchIndex(relId, &prevRecId);

    // Variables to track the current block and slot in the relation
    int block, slot;

    // Check if the previous record ID is invalid, indicating the search should start from the beginning
    if (prevRecId.block == -1 && prevRecId.slot == -1) {
        // Fetch the relation catalog entry for the given relation ID
        RelCatEntry relCatBuf;
        RelCacheTable::getRelCatEntry(relId, &relCatBuf);

        // Start from the first block and slot of the relation
        block = relCatBuf.firstBlk;
        slot = 0;
    }
    else {
        // If there is a valid previous search index, continue from the next slot
        block = prevRecId.block;
        slot = prevRecId.slot + 1;
    }

    // Loop through each block until the end of the relation is reached
    while (block != -1) {
        // Create a record buffer for the current block
        RecBuffer recBuffer(block);

        // Retrieve the header information from the record buffer
        HeadInfo header;
        recBuffer.getHeader(&header);
        
        // Array to hold attribute values of a record
        Attribute record[header.numAttrs];
        recBuffer.getRecord(record, slot);
        
        // Slot map to track the occupancy status of each slot in the block
        unsigned char slotMap[header.numSlots];
        recBuffer.getSlotMap(slotMap);

        // Check if the current slot exceeds the number of slots in the block
        if (slot >= header.numSlots) {
            // Move to the next block and reset the slot counter
            block = header.rblock;
            slot = 0;
            continue;
        }

        // If the current slot is unoccupied, move to the next slot
        if (slotMap[slot] == SLOT_UNOCCUPIED) {
            slot++;
            continue;
        }

        // Fetch the attribute catalog entry for the specified attribute name
        AttrCatEntry attrCatBuf;
        int ret = AttrCacheTable::getAttrCatEntry(relId, attrName, &attrCatBuf);

        // Compare the attribute value of the record with the given attribute value
        int cmpVal = compareAttrs(record[attrCatBuf.offset], attrVal, attrCatBuf.attrType);

        // Check if the comparison result satisfies the given operation (op)
        if (
            (op == NE && cmpVal != 0) ||    // If the operation is "not equal to"
            (op == LT && cmpVal < 0) ||     // If the operation is "less than"
            (op == LE && cmpVal <= 0) ||    // If the operation is "less than or equal to"
            (op == EQ && cmpVal == 0) ||    // If the operation is "equal to"
            (op == GT && cmpVal > 0) ||     // If the operation is "greater than"
            (op == GE && cmpVal >= 0)       // If the operation is "greater than or equal to"
        ) {
            // Create a RecId for the matching record and update the search index
            RecId searchIndex = {block, slot};
            RelCacheTable::setSearchIndex(relId, &searchIndex);
            return searchIndex; // Return the matching record ID
        }
        
        // Move to the next slot in the block
        slot++;
    }

    // Return an invalid record ID if no matching record is found
    return RecId({-1, -1});
}

int BlockAccess::renameRelation(char oldName[ATTR_SIZE], char newName[ATTR_SIZE]) {
    // Reset the search index of the relation catalog
    RelCacheTable::resetSearchIndex(RELCAT_RELID);

    // Set newRelationName with newName
    Attribute newRelationName;
    memcpy(newRelationName.sVal, newName, ATTR_SIZE);

    // Set oldRelationName with oldName
    Attribute oldRelationName;
    memcpy(oldRelationName.sVal, oldName, ATTR_SIZE);

    // Search the relation catalog for an entry with "RelName" = newRelationName
    RecId recId = BlockAccess::linearSearch(RELCAT_RELID, (char*)RELCAT_ATTR_RELNAME, newRelationName, EQ);

    // If a relation with name newName already exists, return E_RELEXIST
    if (recId.block != -1 || recId.slot != -1)
        return E_RELEXIST;

    // Reset the search index of the relation catalog
    RelCacheTable::resetSearchIndex(RELCAT_RELID);

    // Search the relation catalog for an entry with "RelName" = oldRelationName
    recId = BlockAccess::linearSearch(RELCAT_RELID, (char*)RELCAT_ATTR_RELNAME, oldRelationName, EQ);

    // If the relation with name oldName does not exist, return E_RELNOTEXIST
    if (recId.block == -1 && recId.slot == -1)
        return E_RELNOTEXIST;

    // Get the relation catalog record of the relation to rename
    RecBuffer recBuffer(recId.block);

    Attribute record[RELCAT_NO_ATTRS];
    recBuffer.getRecord(record, recId.slot);

    // Update the relation name attribute in the record with newName
    memcpy(&record[RELCAT_REL_NAME_INDEX], &newRelationName, ATTR_SIZE);

    // Set back the updated record
    recBuffer.setRecord(record, recId.slot);

    // Update all the attribute catalog entries for the relation
    RelCacheTable::resetSearchIndex(ATTRCAT_RELID);

    while (true) {
        // Linear search on the attribute catalog for relName = oldRelationName
        RecId attrEntryId = BlockAccess::linearSearch(ATTRCAT_RELID, (char*)ATTRCAT_ATTR_RELNAME, oldRelationName, EQ);

        // If there are no more attributes left to check, break the loop
        if (attrEntryId.block == -1 && attrEntryId.slot == -1)
            break;

        // Get the attribute catalog record
        RecBuffer attrCatRecBuffer(attrEntryId.block);

        Attribute attrCatRecord[ATTRCAT_NO_ATTRS];
        attrCatRecBuffer.getRecord(attrCatRecord, attrEntryId.slot);

        // Update the relName field in the record to newName
        memcpy(&attrCatRecord[ATTRCAT_REL_NAME_INDEX], &newRelationName, ATTR_SIZE);

        // Set back the updated record
        attrCatRecBuffer.setRecord(attrCatRecord, attrEntryId.slot);
    }

    // Return success status
    return SUCCESS;
}

int BlockAccess::renameAttribute(char relName[ATTR_SIZE], char oldName[ATTR_SIZE], char newName[ATTR_SIZE]) {

    // Reset the search index of the relation catalog
    RelCacheTable::resetSearchIndex(RELCAT_RELID);

    // Set relNameAttr to relName
    Attribute relNameAttr;
    memcpy(relNameAttr.sVal, relName, ATTR_SIZE);

    // Search for the relation with name relName in the relation catalog
    RecId recId = BlockAccess::linearSearch(RELCAT_RELID, (char*)RELCAT_ATTR_RELNAME, relNameAttr, EQ);

    // If the relation with name relName does not exist, return E_RELNOTEXIST
    if (recId.block == -1 && recId.slot == -1) 
        return E_RELNOTEXIST;

    // Reset the search index of the attribute catalog
    RelCacheTable::resetSearchIndex(ATTRCAT_RELID); 

    // Variable to store the RecId of the attribute to rename
    RecId attrToRenameId = {-1, -1};

    // Iterate over all attribute catalog entries for the relation
    while(true) {
        // Linear search on the attribute catalog for RelName = relNameAttr
        RecId attrRecId = BlockAccess::linearSearch(ATTRCAT_RELID, (char*)ATTRCAT_ATTR_RELNAME, relNameAttr, EQ);

        // If there are no more attributes left to check, break
        if (attrRecId.block == -1 && attrRecId.slot == -1)
            break;

        // Get the attribute catalog record
        RecBuffer recBuffer(attrRecId.block);
        Attribute record[ATTRCAT_NO_ATTRS];
        recBuffer.getRecord(record, attrRecId.slot);

        // Extract the attribute name from the record
        char attrName[ATTR_SIZE];
        memcpy(attrName, record[ATTRCAT_ATTR_NAME_INDEX].sVal, ATTR_SIZE);

        // If attrName matches oldName, store its RecId
        if (strcmp(attrName, oldName) == 0)
            attrToRenameId = attrRecId;

        // If attrName matches newName, return E_ATTREXIST
        if (strcmp(attrName, newName) == 0)
            return E_ATTREXIST;
    }

    // If no attribute with oldName was found, return E_ATTRNOTEXIST
    if (attrToRenameId.block == -1 && attrToRenameId.slot == -1)
        return E_ATTRNOTEXIST;

    // Update the attribute catalog entry for the attribute
    RecBuffer bufferToRename(attrToRenameId.block);
    Attribute recordToRename[ATTRCAT_NO_ATTRS];

    bufferToRename.getRecord(recordToRename, attrToRenameId.slot);

    // Update the AttrName of the record with newName
    memcpy(recordToRename[ATTRCAT_ATTR_NAME_INDEX].sVal, newName, ATTR_SIZE);

    // Set back the updated record
    bufferToRename.setRecord(recordToRename, attrToRenameId.slot);

    // Return success status
    return SUCCESS;
}

int BlockAccess::insert(int relId, Attribute* record) {
    // Get the relation catalog entry from the relation cache
    RelCatEntry relCatBuf;
    int ret = RelCacheTable::getRelCatEntry(relId, &relCatBuf);

    // If retrieval fails, return the error code
    if (ret != SUCCESS)
        return ret;

    // Get the first record block of the relation from the rel-cat entry
    int blockNum = relCatBuf.firstBlk;

    // Variables to store the RecId where the new record will be inserted
    RecId recId = {-1, -1};

    // Get the number of slots per block and the number of attributes of the relation
    int numSlots = relCatBuf.numSlotsPerBlk;
    int numAttrs = relCatBuf.numAttrs;

    // Initialize prevBlockNum as -1
    int prevBlockNum = -1;

    // Traverse the linked list of existing record blocks until a free slot is found or end of list
    while (blockNum != -1) {
        // Create a RecBuffer object for the current block
        RecBuffer currentBlock(blockNum);

        // Get the header of the current block
        HeadInfo currentHeader;
        currentBlock.getHeader(&currentHeader);

        // Get the slot map of the current block
        unsigned char slotMap[numSlots];
        currentBlock.getSlotMap(slotMap);
        
        // Search for a free slot in the current block
        int freeSlot = -1;
        for (int i = 0; i < numSlots; i++) {
            if (slotMap[i] == SLOT_UNOCCUPIED) {
                freeSlot = i;
                break; 
            }
        }

        // If a free slot is found, set recId and break the loop
        if (freeSlot != -1) {
            recId.block = blockNum;
            recId.slot = freeSlot;
            break;
        }

        // Otherwise, move to the next block
        prevBlockNum = blockNum;
        blockNum = currentHeader.rblock;
    }

    // If no free slot is found in existing blocks
    if (recId.block == -1 || recId.slot == -1) {
        // If the relation is RELCAT, do not allocate more blocks, return E_MAXRELATIONS
        if (relId == RELCAT_RELID)
            return E_MAXRELATIONS;
        
        // Otherwise, allocate a new record block
        RecBuffer newBlock;

        // Get the block number of the newly allocated block
        int newBlockNum = newBlock.getBlockNum();

        // If allocation fails due to disk full, return E_DISKFULL
        if (newBlockNum == E_DISKFULL)
            return E_DISKFULL;

        // Set recId to point to the new block and slot 0
        recId.block = newBlockNum;
        recId.slot = 0;

        // Set up the header of the new block
        HeadInfo newBlockHeader;
        newBlock.getHeader(&newBlockHeader);
        newBlockHeader.lblock = prevBlockNum;
        newBlockHeader.numAttrs = numAttrs;
        newBlockHeader.numSlots = numSlots;
        newBlock.setHeader(&newBlockHeader);

        // Initialize the slot map of the new block
        unsigned char newBlockSlotMap[numSlots];
        for (int i = 0; i < numSlots; i++)
            newBlockSlotMap[i] = SLOT_UNOCCUPIED;
        newBlock.setSlotMap(newBlockSlotMap);

        // If prevBlockNum is valid, update the previous block's rblock
        if (prevBlockNum != -1) {
            RecBuffer prevBlock(prevBlockNum);

            HeadInfo prevBlockHeader;
            prevBlock.getHeader(&prevBlockHeader);
            prevBlockHeader.rblock = recId.block;
            prevBlock.setHeader(&prevBlockHeader);
        }
        else {
            // Otherwise, update the firstBlk and lastBlk in the rel-cat entry
            relCatBuf.firstBlk = recId.block;
            relCatBuf.lastBlk = recId.block;
            RelCacheTable::setRelCatEntry(relId, &relCatBuf);
        }
    }

    // Create a RecBuffer object for the block where the record will be inserted
    RecBuffer blockToInsert(recId.block);

    // Insert the record into the specified slot
    blockToInsert.setRecord(record, recId.slot);

    // Update the slot map of the block
    unsigned char slotMapToInsert[numSlots];
    blockToInsert.getSlotMap(slotMapToInsert);
    slotMapToInsert[recId.slot] = SLOT_OCCUPIED;
    blockToInsert.setSlotMap(slotMapToInsert);

    // Increment the numEntries in the block header
    HeadInfo headerToInsert;
    blockToInsert.getHeader(&headerToInsert);
    headerToInsert.numEntries++;
    blockToInsert.setHeader(&headerToInsert);

    // Increment the numRecs in the relation catalog entry
    relCatBuf.numRecs++;
    RelCacheTable::setRelCatEntry(relId, &relCatBuf);

    // Variable to track if any index blocks were released
    int flag = SUCCESS;

    // Iterate over all attributes of the relation
    for (int attrOffset = 0; attrOffset < numAttrs; attrOffset++) {
        // Get the attribute catalog entry for the attribute
        AttrCatEntry attrCatBuf;
        AttrCacheTable::getAttrCatEntry(relId, attrOffset, &attrCatBuf);
        if (attrCatBuf.rootBlock == -1)
            continue;
        
        // If an index exists for the attribute, insert into the B+ tree
        int ret = BPlusTree::bPlusInsert(relId, attrCatBuf.attrName, record[attrOffset], recId);
        if (ret == E_DISKFULL)
            flag = E_INDEX_BLOCKS_RELEASED;
    }

    // Return the appropriate status
    return flag;
}

int BlockAccess::deleteRelation(char relName[ATTR_SIZE]) {
    // Check if the relation to delete is RELCAT or ATTRCAT
    if (
        strcmp(relName, (char*)RELCAT_RELNAME) == 0 ||
        strcmp(relName, (char*)ATTRCAT_RELNAME) == 0
    ) {
        return E_NOTPERMITTED;
    }

    // Reset the search index of the relation catalog
    RelCacheTable::resetSearchIndex(RELCAT_RELID);

    // Create an Attribute to hold the relation name
    Attribute relNameAttribute;
    strcpy(relNameAttribute.sVal, relName);

    // Search the relation catalog for the relation
    RecId recId = linearSearch(RELCAT_RELID, (char*)RELCAT_ATTR_RELNAME, relNameAttribute, EQ);

    // If the relation does not exist, return E_RELNOTEXIST
    if (recId.block == -1 || recId.slot == -1)
        return E_RELNOTEXIST;

    // Retrieve the relation catalog entry record
    Attribute relCatEntryRecord[RELCAT_NO_ATTRS];

    RecBuffer recBuffer(recId.block);
    recBuffer.getRecord(relCatEntryRecord, recId.slot);

    // Get the first record block of the relation
    int currentBlock = relCatEntryRecord[RELCAT_FIRST_BLOCK_INDEX].nVal;

    // Delete all the record blocks of the relation
    while(currentBlock != -1) {
        RecBuffer currentBlockBuffer(currentBlock);
        HeadInfo currentBlockHeader;
        currentBlockBuffer.getHeader(&currentBlockHeader);

        int nextBlock = currentBlockHeader.rblock;

        // Release the current block
        currentBlockBuffer.releaseBlock();
        currentBlock = nextBlock;
    }

    // Delete attribute catalog entries and associated index trees
    int numAttrsDeleted = 0;
    RelCacheTable::resetSearchIndex(ATTRCAT_RELID);
    while(true) {
        // Linear search on the attribute catalog for relName
        RecId attrCatRecId = linearSearch(ATTRCAT_RELID, (char*)ATTRCAT_ATTR_RELNAME, relNameAttribute, EQ);

        // If no more attributes, break
        if (attrCatRecId.slot == -1 || attrCatRecId.block == -1)
            break;

        numAttrsDeleted++;

        // Get the attribute catalog record
        RecBuffer currentBlock(attrCatRecId.block);

        HeadInfo currentBlockHeader;
        currentBlock.getHeader(&currentBlockHeader);

        Attribute record[ATTRCAT_NO_ATTRS];
        currentBlock.getRecord(record, attrCatRecId.slot);

        // Get the root block of the index (if any)
        int rootBlock = record[ATTRCAT_ROOT_BLOCK_INDEX].nVal;

        // Mark the slot as unoccupied
        unsigned char slotMap[currentBlockHeader.numSlots];
        currentBlock.getSlotMap(slotMap);
        slotMap[attrCatRecId.slot] = SLOT_UNOCCUPIED;
        currentBlock.setSlotMap(slotMap);

        // Decrement the numEntries in the block header
        currentBlockHeader.numEntries--;
        currentBlock.setHeader(&currentBlockHeader);

        // If the block is empty, release it and update linked list
        if (currentBlockHeader.numEntries == 0) {
            int leftBlock = currentBlockHeader.lblock;
            int rightBlock = currentBlockHeader.rblock;
            
            // Update left block's rblock
            if (leftBlock != -1) {
                RecBuffer prevBlock(leftBlock);
                HeadInfo prevBlockHeader;

                prevBlock.getHeader(&prevBlockHeader);
                prevBlockHeader.rblock = rightBlock;
                prevBlock.setHeader(&prevBlockHeader);
            }

            // Update right block's lblock
            if (rightBlock != -1) {
                RecBuffer nextBlock(rightBlock);
                HeadInfo nextBlockHeader;

                nextBlock.getHeader(&nextBlockHeader);
                nextBlockHeader.lblock = leftBlock;
                nextBlock.setHeader(&nextBlockHeader);
            }

            // Release the current block
            currentBlock.releaseBlock();
        }

        // If an index exists for the attribute, destroy the B+ tree
        if (rootBlock != -1)
            BPlusTree::bPlusDestroy(rootBlock);
    }

    // Delete the entry from the relation catalog
    HeadInfo relCatHeader;
    recBuffer.getHeader(&relCatHeader);

    unsigned char recSlotMap[relCatHeader.numSlots];

    recBuffer.getSlotMap(recSlotMap);
    recSlotMap[recId.slot] = SLOT_UNOCCUPIED;
    recBuffer.setSlotMap(recSlotMap);

    relCatHeader.numEntries--;
    recBuffer.setHeader(&relCatHeader);

    // Update the relation cache entries for RELCAT and ATTRCAT
    RelCatEntry relCatBuf;
    RelCacheTable::getRelCatEntry(RELCAT_RELID, &relCatBuf);
    relCatBuf.numRecs--;
    RelCacheTable::setRelCatEntry(RELCAT_RELID, &relCatBuf);

    RelCatEntry attrCatBuf;
    RelCacheTable::getRelCatEntry(ATTRCAT_RELID, &attrCatBuf);
    attrCatBuf.numRecs -= numAttrsDeleted;
    RelCacheTable::setRelCatEntry(ATTRCAT_RELID, &attrCatBuf);

    // Return success
    return SUCCESS;
}

/**
 * Retrieves the next record from the specified relation in a sequential scan.
 * @param relId The ID of the relation to scan.
 * @param record Output parameter to store the next record.
 * @return `SUCCESS` if a record is found; otherwise, returns `E_NOTFOUND`.
 */
int BlockAccess::project(int relId, Attribute* record) {
    // Get the previous search index of the relation
    RecId prevRecId;
    RelCacheTable::getSearchIndex(relId, &prevRecId);

    // Variables to store the current block and slot
    int block, slot;

    // If the search index is invalid, start from the beginning
    if (prevRecId.block == -1 && prevRecId.slot == -1) {
        // Get the first record block of the relation
        RelCatEntry relCatEntry;
        RelCacheTable::getRelCatEntry(relId, &relCatEntry);

        block = relCatEntry.firstBlk;
        slot = 0;
    }
    else {
        // Continue from the next slot
        block = prevRecId.block;
        slot = prevRecId.slot+1;
    }

    // Find the next occupied slot
    while (block != -1) {
        RecBuffer currentBlock(block);

        HeadInfo currentHeader;
        currentBlock.getHeader(&currentHeader);

        unsigned char slotMap[currentHeader.numSlots];
        currentBlock.getSlotMap(slotMap);

        if (slot >= currentHeader.numSlots) {
            // Move to the next block
            block = currentHeader.rblock;
            slot = 0;
        }
        else if (slotMap[slot] == SLOT_UNOCCUPIED) {
            // Skip unoccupied slots
            slot++;
        }
        else {
            // Found an occupied slot
            break;
        }
    }

    // If no more records, return E_NOTFOUND
    if (block == -1)
        return E_NOTFOUND;

    // Create a RecId for the found record
    RecId nextRecId = {block, slot};

    // Update the search index
    RelCacheTable::setSearchIndex(relId, &nextRecId);

    // Retrieve the record
    RecBuffer targetBlock(block);
    targetBlock.getRecord(record, slot);

    // Return success
    return SUCCESS;
}

int BlockAccess::search(int relId, Attribute* record, char attrName[ATTR_SIZE], Attribute attrVal, int op) {
    // Declare a variable to store the RecId of the found record
    RecId recId;

    // Get the attribute catalog entry for the specified attribute
    AttrCatEntry attrCatBuf;
    int ret = AttrCacheTable::getAttrCatEntry(relId, attrName, &attrCatBuf);
    if (ret != SUCCESS)
        return ret;

    // Get the root block of the index for the attribute
    int rootBlock = attrCatBuf.rootBlock;

    // If an index exists for the attribute, use B+ tree search
    if (rootBlock != -1)
        recId = BPlusTree::bPlusSearch(relId, attrName, attrVal, op);
    else
        // Otherwise, perform a linear search
        recId = linearSearch(relId, attrName, attrVal, op);

    // If no matching record is found, return E_NOTFOUND
    if (recId.block == -1 || recId.slot == -1)
        return E_NOTFOUND;

    // Retrieve the record from the found RecId
    RecBuffer recBuffer(recId.block);
    recBuffer.getRecord(record, recId.slot);

    // Return success
    return SUCCESS;
}