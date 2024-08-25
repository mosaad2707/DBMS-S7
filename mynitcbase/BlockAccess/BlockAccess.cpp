#include "BlockAccess.h"
#include <cstring>
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
