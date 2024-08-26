#include "BlockAccess.h"
#include <cstring>
#include "BlockAccess.h"
#include <cstring>
#include<iostream>
#include<stdio.h>


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
        
        // Slot map to track the occupancy status of each slot in the block.
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
    // Reset the search index of the relation catalog using RelCacheTable::resetSearchIndex()
    RelCacheTable::resetSearchIndex(RELCAT_RELID);

    // Set newRelationName with newName
    Attribute newRelationName;
    memcpy(newRelationName.sVal, newName, ATTR_SIZE);

    // Set oldRelationName with oldName
    Attribute oldRelationName;
    memcpy(oldRelationName.sVal, oldName, ATTR_SIZE);

    // Search the relation catalog for an entry with "RelName" = newRelationName
    RecId recId = BlockAccess::linearSearch(RELCAT_RELID, (char*)RELCAT_ATTR_RELNAME, newRelationName, EQ);

    // If a relation with name newName already exists (result of linearSearch is not {-1, -1}), return E_RELEXIST
    if (recId.block != -1 || recId.slot != -1)
        return E_RELEXIST;

    // Reset the search index of the relation catalog using RelCacheTable::resetSearchIndex()
    RelCacheTable::resetSearchIndex(RELCAT_RELID);

    // Search the relation catalog for an entry with "RelName" = oldRelationName
    recId = BlockAccess::linearSearch(RELCAT_RELID, (char*)RELCAT_ATTR_RELNAME, oldRelationName, EQ);

    // If relation with name oldName does not exist (result of linearSearch is {-1, -1}), return E_RELNOTEXIST
    if (recId.block == -1 && recId.slot == -1)
        return E_RELNOTEXIST;

    // Get the relation catalog record of the relation to rename using a RecBuffer on the relation catalog [RELCAT_BLOCK] 
    // and RecBuffer.getRecord function
    RecBuffer recBuffer(recId.block);

    Attribute record[RELCAT_NO_ATTRS];
    recBuffer.getRecord(record, recId.slot);

    // Update the relation name attribute in the record with newName (use RELCAT_REL_NAME_INDEX)
    memcpy(&record[RELCAT_REL_NAME_INDEX], &newRelationName, ATTR_SIZE);

    // Set back the record value using RecBuffer.setRecord
    recBuffer.setRecord(record, recId.slot);

    // Update all the attribute catalog entries in the attribute catalog corresponding
    // to the relation with relation name oldName to the relation name newName
    RelCacheTable::resetSearchIndex(ATTRCAT_RELID);

    while (true) {
        // LinearSearch on the attribute catalog for relName = oldRelationName
        RecId attrEntryId = BlockAccess::linearSearch(ATTRCAT_RELID, (char*)ATTRCAT_ATTR_RELNAME, oldRelationName, EQ);

        // If there are no more attributes left to check (linearSearch returned {-1, -1}), break the loop
        if (attrEntryId.block == -1 && attrEntryId.slot == -1)
            break;

        // Get the record using RecBuffer.getRecord
        RecBuffer attrCatRecBuffer(attrEntryId.block);
        Attribute attrCatRecord[ATTRCAT_NO_ATTRS];
        attrCatRecBuffer.getRecord(attrCatRecord, attrEntryId.slot);

        // Update the relName field in the record to newName
        memcpy(&attrCatRecord[ATTRCAT_REL_NAME_INDEX], &newRelationName, ATTR_SIZE);

        // Set back the record using RecBuffer.setRecord
        attrCatRecBuffer.setRecord(attrCatRecord, attrEntryId.slot);
    }

    // Return success status
    return SUCCESS;
}

int BlockAccess::renameAttribute(char relName[ATTR_SIZE], char oldName[ATTR_SIZE], char newName[ATTR_SIZE]) {

    // Reset the search index of the relation catalog using RelCacheTable::resetSearchIndex()
    RelCacheTable::resetSearchIndex(RELCAT_RELID);

    // Set relNameAttr to relName
    Attribute relNameAttr;
    memcpy(relNameAttr.sVal, relName, ATTR_SIZE);

    // Search for the relation with name relName in the relation catalog using linearSearch()
    RecId recId = BlockAccess::linearSearch(RELCAT_RELID, (char*)RELCAT_ATTR_RELNAME, relNameAttr, EQ);

    // If the relation with name relName does not exist (search returns {-1,-1}), return E_RELNOTEXIST
    if (recId.block == -1 && recId.slot == -1)
        return E_RELNOTEXIST;

    // Reset the search index of the attribute catalog using RelCacheTable::resetSearchIndex()
    RelCacheTable::resetSearchIndex(ATTRCAT_RELID);

    // Declare variable attrToRenameRecId used to store the attr-cat recId of the attribute to rename
    RecId attrToRenameId = {-1, -1};

    // Iterate over all Attribute Catalog Entry records corresponding to the relation to find the required attribute
    while (true) {
        // Linear search on the attribute catalog for RelName = relNameAttr
        RecId attrRecId = BlockAccess::linearSearch(ATTRCAT_RELID, (char*)ATTRCAT_ATTR_RELNAME, relNameAttr, EQ);

        // If there are no more attributes left to check (linearSearch returned {-1,-1}), break
        if (attrRecId.block == -1 && attrRecId.slot == -1)
            break;

        // Get the record from the attribute catalog using RecBuffer.getRecord into attrCatEntryRecord
        RecBuffer recBuffer(attrRecId.block);
        Attribute record[ATTRCAT_NO_ATTRS];
        recBuffer.getRecord(record, attrRecId.slot);

        // Extract the attribute name from the record
        char attrName[ATTR_SIZE];
        memcpy(attrName, record[ATTRCAT_ATTR_NAME_INDEX].sVal, ATTR_SIZE);

        // If attrCatEntryRecord.attrName = oldName, store its recId for renaming
        if (strcmp(attrName, oldName) == 0)
            attrToRenameId = attrRecId;

        // If attrCatEntryRecord.attrName = newName, return E_ATTREXIST as the new name already exists
        if (strcmp(attrName, newName) == 0)
            return E_ATTREXIST;
    }

    // If no attribute with the old name was found, return E_ATTRNOTEXIST
    if (attrToRenameId.block == -1 && attrToRenameId.slot == -1)
        return E_ATTRNOTEXIST;

    // Update the entry corresponding to the attribute in the Attribute Catalog Relation.
    // Declare a RecBuffer for attrToRenameRecId.block and get the record at attrToRenameRecId.slot
    RecBuffer bufferToRename(attrToRenameId.block);
    Attribute recordToRename[ATTRCAT_NO_ATTRS];
    bufferToRename.getRecord(recordToRename, attrToRenameId.slot);

    // Update the AttrName of the record with newName
    memcpy(recordToRename[ATTRCAT_ATTR_NAME_INDEX].sVal, newName, ATTR_SIZE);

    // Set back the updated record using RecBuffer.setRecord
    bufferToRename.setRecord(recordToRename, attrToRenameId.slot);

    // Return success status
    return SUCCESS;
}

//Stage 7

int BlockAccess::insert(int relId, Attribute* record) {
    // Get the relation catalog entry from the relation cache
    // (Use RelCacheTable::getRelCatEntry() of Cache Layer)
    RelCatEntry relCatBuf;
    int ret = RelCacheTable::getRelCatEntry(relId, &relCatBuf);
    // printf("ret inside insert: %d\n", ret);

    if (ret != SUCCESS)
        return ret;
    // printf("relCatBuf.relName: %s\n", relCatBuf.relName);
    // Get the first record block of the relation from the rel-cat entry
    int blockNum = relCatBuf.firstBlk;
    // printf("blockNum: %d\n", blockNum);

    // rec_id will be used to store where the new record will be inserted
    RecId recId = {-1, -1};

    // Get the number of slots per record block and the number of attributes of the relation
    int numSlots = relCatBuf.numSlotsPerBlk;
    int numAttrs = relCatBuf.numAttrs;

    // Initialize prevBlockNum as -1, which will be used to store the block number of the last element in the linked list
    int prevBlockNum = -1;

    /*
        Traversing the linked list of existing record blocks of the relation
        until a free slot is found OR until the end of the list is reached
    */
    while (blockNum != -1) {
        // Create a RecBuffer object for blockNum
        RecBuffer currentBlock(blockNum);

        // Get the header of block(blockNum) using RecBuffer::getHeader() function
        HeadInfo currentHeader;
        currentBlock.getHeader(&currentHeader);

        // Get the slot map of block(blockNum) using RecBuffer::getSlotMap() function
        unsigned char slotMap[numSlots];
        currentBlock.getSlotMap(slotMap);
        
        // Search for a free slot in the block 'blockNum' and store its rec-id in rec_id
        int freeSlot = -1;
        for (int i = 0; i < numSlots; i++) {
            // printf("slotMap[%d]: %d\n", i, slotMap[i]);
            if (slotMap[i] == SLOT_UNOCCUPIED) {
                freeSlot = i;
                break; 
            }
        }

        /* If a free slot is found, set rec_id and discontinue the traversal
           of the linked list of record blocks (break from the loop) */
        if (freeSlot != -1) {
            recId.block = blockNum;
            recId.slot = freeSlot;
            break;
        }

        /* Otherwise, continue to check the next block by updating the block numbers as follows:
           update prevBlockNum = blockNum
           update blockNum = header.rblock (next element in the linked list of record blocks) */
        prevBlockNum = blockNum;
        blockNum = currentHeader.rblock;
    }
    // printf("recId.block: %d\n", recId.block);
    // printf("recId.slot: %d\n", recId.slot);

    // If no free slot is found in existing record blocks (rec_id = {-1, -1})
    if (recId.block == -1 || recId.slot == -1) {
        // If the relation is RELCAT, do not allocate any more blocks, return E_MAXRELATIONS
        if (relId == RELCAT_RELID)
            return E_MAXRELATIONS;
        
        // Otherwise, get a new record block using the appropriate RecBuffer constructor
        RecBuffer newBlock;

        // Get the block number of the newly allocated block
        int newBlockNum = newBlock.getBlockNum();

        if (newBlockNum == E_DISKFULL)
            return E_DISKFULL;

        // Assign rec_id.block = new block number and rec_id.slot = 0
        recId.block = newBlockNum;
        recId.slot = 0;
        // printf("newBlockNum: %d\n", newBlockNum);
        // printf("recId.block: %d\n", recId.block);


        /*
            Set the header of the new record block such that it links with
            existing record blocks of the relation. Set the block's header as follows:
            blockType: REC, pblock: -1
            lblock = -1 (if linked list of existing record blocks was empty, i.e., this is the first insertion into the relation)
            lblock = prevBlockNum (otherwise),
            rblock: -1, numEntries: 0,
            numSlots: numSlots, numAttrs: numAttrs
            (Use BlockBuffer::setHeader() function)
        */
        HeadInfo newBlockHeader;
        newBlock.getHeader(&newBlockHeader);
        newBlockHeader.lblock = prevBlockNum;
        newBlockHeader.numAttrs = numAttrs;
        newBlockHeader.numSlots = numSlots;
        newBlock.setHeader(&newBlockHeader);

        /*
            Set the block's slot map with all slots marked as free
            (i.e., store SLOT_UNOCCUPIED for all the entries)
            (Use RecBuffer::setSlotMap() function)
        */
        unsigned char newBlockSlotMap[numSlots];
        newBlock.getSlotMap(newBlockSlotMap);
        for (int i = 0; i < numSlots; i++)
            newBlockSlotMap[i] = SLOT_UNOCCUPIED;
        newBlock.setSlotMap(newBlockSlotMap);

        // If prevBlockNum != -1, update the previous block
        if (prevBlockNum != -1) {
            RecBuffer prevBlock(prevBlockNum);

            HeadInfo prevBlockHeader;
            prevBlock.getHeader(&prevBlockHeader);
            prevBlockHeader.rblock = recId.block;
            prevBlock.setHeader(&prevBlockHeader);
        }
        // Otherwise, update the first block field in the relation catalog entry to the new block
        else {
            relCatBuf.firstBlk = recId.block;
            relCatBuf.lastBlk = recId.block;
            RelCacheTable::setRelCatEntry(relId, &relCatBuf);
        }
    }

    // Create a RecBuffer object for rec_id.block
    RecBuffer blockToInsert(recId.block);

    // Insert the record into rec_id's slot using RecBuffer.setRecord()
    blockToInsert.setRecord(record, recId.slot);

    /* Update the slot map of the block by marking the entry of the slot to which the record was inserted as occupied
       (i.e., store SLOT_OCCUPIED in free_slot's entry of slot map)
       (Use RecBuffer::getSlotMap() and RecBuffer::setSlotMap() functions) */
    unsigned char slotMapToInsert[numSlots];
    blockToInsert.getSlotMap(slotMapToInsert);
    slotMapToInsert[recId.slot] = SLOT_OCCUPIED;
    blockToInsert.setSlotMap(slotMapToInsert);

    // Increment the numEntries field in the header of the block to which the record was inserted
    // (Use BlockBuffer::getHeader() and BlockBuffer::setHeader() functions)
    HeadInfo headerToInsert;
    blockToInsert.getHeader(&headerToInsert);
    headerToInsert.numEntries++;
    blockToInsert.setHeader(&headerToInsert);

    // Increment the number of records field in the relation cache entry for the relation
    // (Use RelCacheTable::setRelCatEntry function)
    relCatBuf.numRecs++;
    RelCacheTable::setRelCatEntry(relId, &relCatBuf);

    // Return SUCCESS
    return SUCCESS;
}
