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

/*
NOTE: This function will copy the result of the search to the `record` argument.
      The caller should ensure that space is allocated for `record` array
      based on the number of attributes in the relation.
*/
int BlockAccess::search(int relId, Attribute *record, char attrName[ATTR_SIZE], Attribute attrVal, int op) {
    // Declare a variable called recid to store the searched record
    RecId recId;

    /* search for the record id (recid) corresponding to the attribute with
    attribute name attrName, with value attrval and satisfying the condition op
    using linearSearch() */
    recId = BlockAccess::linearSearch(relId, attrName, attrVal, op);

    // if there's no record satisfying the given condition (recId = {-1, -1})
    //    return E_NOTFOUND;
    if (recId.block == -1 && recId.slot == -1)
        return E_NOTFOUND;

    /* Copy the record with record id (recId) to the record buffer (record)
       For this Instantiate a RecBuffer class object using recId and
       call the appropriate method to fetch the record
    */
    RecBuffer recBuffer(recId.block);
    recBuffer.getRecord(record, recId.slot);


    return SUCCESS;
}

int BlockAccess::deleteRelation(char relName[ATTR_SIZE]) {
    // if the relation to delete is either Relation Catalog or Attribute Catalog,
    //     return E_NOTPERMITTED
        // (check if the relation names are either "RELATIONCAT" and "ATTRIBUTECAT".
        // you may use the following constants: RELCAT_NAME and ATTRCAT_NAME)
    if(strcmp(relName, RELCAT_RELNAME) == 0 || strcmp(relName, ATTRCAT_RELNAME) == 0)
        return E_NOTPERMITTED;

    /* reset the searchIndex of the relation catalog using
       RelCacheTable::resetSearchIndex() */
    RelCacheTable::resetSearchIndex(RELCAT_RELID);
    Attribute relNameAttr; // (stores relName as type union Attribute)
    // assign relNameAttr.sVal = relName
    memcpy(relNameAttr.sVal, relName, ATTR_SIZE);

    //  linearSearch on the relation catalog for RelName = relNameAttr
    RecId relCatRecId = BlockAccess::linearSearch(RELCAT_RELID, (char*)RELCAT_ATTR_RELNAME, relNameAttr, EQ);

    // if the relation does not exist (linearSearch returned {-1, -1})
    //     return E_RELNOTEXIST
    if (relCatRecId.block == -1 && relCatRecId.slot == -1)
        return E_RELNOTEXIST;

    Attribute relCatEntryRecord[RELCAT_NO_ATTRS];
    /* store the relation catalog record corresponding to the relation in
       relCatEntryRecord using RecBuffer.getRecord */
    RecBuffer relCatRecBuffer(relCatRecId.block);  
    relCatRecBuffer.getRecord(relCatEntryRecord, relCatRecId.slot); 

    /* get the first record block of the relation (firstBlock) using the
       relation catalog entry record */
    int firstBlock= relCatEntryRecord[RELCAT_FIRST_BLOCK_INDEX].nVal;
    int numAttributes = relCatEntryRecord[RELCAT_NO_ATTRIBUTES_INDEX].nVal;
    int currentBlock = firstBlock;
       
    /* get the number of attributes corresponding to the relation (numAttrs)
       using the relation catalog entry record */

    /*
    
     Delete all the record blocks of the relation
    */
    // for each record block of the relation:
    //     get block header using BlockBuffer.getHeader
    //     get the next block from the header (rblock)
    //     release the block using BlockBuffer.releaseBlock
    //
    //     Hint: to know if we reached the end, check if nextBlock = -1
    while(currentBlock != -1) {
        RecBuffer currentBlockBuffer(currentBlock);
        HeadInfo currentBlockHeader;
        currentBlockBuffer.getHeader(&currentBlockHeader);

        int nextBlock = currentBlockHeader.rblock;

        currentBlockBuffer.releaseBlock();
        currentBlock = nextBlock;
    }


    /***
        Deleting attribute catalog entries corresponding the relation and index
        blocks corresponding to the relation with relName on its attributes
    ***/

    // reset the searchIndex of the attribute catalog

    int numberOfAttributesDeleted = 0;

    while(true) {
        // attrCatRecId : `relname`'s entry in `ATTRCAT`
        RecId attrCatRecId = BlockAccess::linearSearch(ATTRCAT_RELID,(char*) RELCAT_ATTR_RELNAME, relNameAttr, EQ);

        // if no more attributes to iterate over (attrCatRecId == {-1, -1})
		if(attrCatRecId.block == -1 && attrCatRecId.slot == -1){
            break;
        }

        numberOfAttributesDeleted++;

        // create a RecBuffer for attrCatRecId.block
        // get the header of the block
        // get the record corresponding to attrCatRecId.slot
		RecBuffer attrCatBlockBuffer (attrCatRecId.block);

		HeadInfo attrCatHeader;
		attrCatBlockBuffer.getHeader(&attrCatHeader);

		Attribute attrCatRecord [ATTRCAT_NO_ATTRS];
		attrCatBlockBuffer.getRecord(attrCatRecord, attrCatRecId.slot);

        // declare variable rootBlock which will be used to store the root
        // block field from the attribute catalog record.
        int rootBlock = attrCatRecord[ATTRCAT_ROOT_BLOCK_INDEX].nVal; // get root block from the record
        // (This will be used later to delete any indexes if it exists)
		
        // Update the Slotmap for the block by setting the slot as SLOT_UNOCCUPIED
        // Hint: use RecBuffer.getSlotMap and RecBuffer.setSlotMap
		unsigned char slotmap [attrCatHeader.numSlots];
		attrCatBlockBuffer.getSlotMap(slotmap);

		slotmap[attrCatRecId.slot] = SLOT_UNOCCUPIED;
		attrCatBlockBuffer.setSlotMap(slotmap);

        /* Decrement the numEntries in the header of the block corresponding to
           the attribute catalog entry and then set back the header
           using RecBuffer.setHeader */

        attrCatHeader.numEntries--;
		attrCatBlockBuffer.setHeader(&attrCatHeader);

        /* If number of entries become 0, releaseBlock is called after fixing
           the linked list.
        */
        if (attrCatHeader.numEntries == 0) {
            /* Standard DOUBLY Linked List Delete for a Block
               Get the header of the left block and set it's rblock to this
               block's rblock
            */

            // create a RecBuffer for lblock and call appropriate methods
			RecBuffer prevBlock (attrCatHeader.lblock);
			
			HeadInfo leftHeader;
			prevBlock.getHeader(&leftHeader);

			leftHeader.rblock = attrCatHeader.rblock;
			prevBlock.setHeader(&leftHeader);


            if (attrCatHeader.rblock != INVALID_BLOCKNUM) 
			{
                /* Get the header of the right block and set it's lblock to
                   this block's lblock */
                // create a RecBuffer for rblock and call appropriate methods
				RecBuffer nextBlock (attrCatHeader.rblock);
				
				HeadInfo rightHeader;
				nextBlock.getHeader(&rightHeader);

				rightHeader.lblock = attrCatHeader.lblock;
				nextBlock.setHeader(&rightHeader);

            }  else 
			{
                // (the block being released is the "Last Block" of the relation.)
                /* update the Relation Catalog entry's LastBlock field for this
                   relation with the block number of the previous block. */

				RelCatEntry relCatEntryBuffer;
				RelCacheTable::getRelCatEntry(ATTRCAT_RELID, &relCatEntryBuffer);

				relCatEntryBuffer.lastBlk = attrCatHeader.lblock;
            }

            // (Since the attribute catalog will never be empty(why?), we do not
            //  need to handle the case of the linked list becoming empty - i.e
            //  every block of the attribute catalog gets released.)

            // call releaseBlock()
			attrCatBlockBuffer.releaseBlock();
        }

		/*
        // (the following part is only relevant once indexing has been implemented)
        // if index exists for the attribute (rootBlock != -1), call bplus destroy
        if (rootBlock != -1) {
            // delete the bplus tree rooted at rootBlock using BPlusTree::bPlusDestroy()
        }
		*/
    }

    /*** Delete the entry corresponding to the relation from relation catalog ***/
    // Fetch the header of Relcat block

	HeadInfo relCatHeader;
    relCatRecBuffer.getHeader(&relCatHeader);
    


    /* Decrement the numEntries in the header of the block corresponding to the
       relation catalog entry and set it back */
    relCatHeader.numEntries--;
    relCatRecBuffer.setHeader(&relCatHeader);

   /* Get the slotmap in relation catalog, update it by marking the slot as
       free(SLOT_UNOCCUPIED) and set it back. */
	unsigned char slotmap [relCatHeader.numSlots];
    relCatRecBuffer.getSlotMap(slotmap);    

	slotmap[relCatRecId.slot] = SLOT_UNOCCUPIED;
    relCatRecBuffer.setSlotMap(slotmap);

    /*** Updating the Relation Cache Table ***/
    /** Update relation catalog record entry (number of records in relation
        catalog is decreased by 1) **/

	// Get the entry corresponding to relation catalog from the relation
    // cache and update the number of records and set it back
    // (using RelCacheTable::setRelCatEntry() function)

	RelCatEntry relCatEntryBuffer;
	RelCacheTable::getRelCatEntry(RELCAT_RELID, &relCatEntryBuffer);

	relCatEntryBuffer.numRecs--;
	RelCacheTable::setRelCatEntry(RELCAT_RELID, &relCatEntryBuffer);

    /** Update attribute catalog entry (number of records in attribute catalog
        is decreased by numberOfAttributesDeleted) **/
    // i.e., #Records = #Records - numberOfAttributesDeleted

    // Get the entry corresponding to attribute catalog from the relation
    // cache and update the number of records and set it back
    // (using RelCacheTable::setRelCatEntry() function)

	RelCacheTable::getRelCatEntry(ATTRCAT_RELID, &relCatEntryBuffer);
	relCatEntryBuffer.numRecs -= numberOfAttributesDeleted;
	RelCacheTable::setRelCatEntry(ATTRCAT_RELID, &relCatEntryBuffer);


    return SUCCESS;
}

/*
NOTE: the caller is expected to allocate space for the argument `record` based
      on the size of the relation. This function will only copy the result of
      the projection onto the array pointed to by the argument.
*/
int BlockAccess::project(int relId, Attribute *record) {
    // get the previous search index of the relation relId from the relation
    // cache (use RelCacheTable::getSearchIndex() function)
    RecId prevRecId;
    RelCacheTable::getSearchIndex(relId, &prevRecId);

    // declare block and slot which will be used to store the record id of the
    // slot we need to check.
    int block, slot;

    /* if the current search index record is invalid(i.e. = {-1, -1})
       (this only happens when the caller reset the search index)
    */
    if (prevRecId.block == -1 && prevRecId.slot == -1)
    {
        // (new project operation. start from beginning)

        // get the first record block of the relation from the relation cache
        // (use RelCacheTable::getRelCatEntry() function of Cache Layer)
        RelCatEntry RelCatEntry;
        RelCacheTable::getRelCatEntry(relId, &RelCatEntry);

        // block = first record block of the relation
        block = RelCatEntry.firstBlk;
        // slot = 0
        slot = 0;
    }
    else
    {
        // (a project/search operation is already in progress)

        // block = previous search index's block
        block = prevRecId.block;
        // slot = previous search index's slot + 1
        slot = prevRecId.slot + 1;
    }


    // The following code finds the next record of the relation
    /* Start from the record id (block, slot) and iterate over the remaining
       records of the relation */
    while (block != -1)
    {
        // create a RecBuffer object for block (using appropriate constructor!)
        RecBuffer recBuffer(block);

        // get header of the block using RecBuffer::getHeader() function
        HeadInfo header;
        recBuffer.getHeader(&header);

        // get slot map of the block using RecBuffer::getSlotMap() function
        unsigned char slotMap[header.numSlots];
        recBuffer.getSlotMap(slotMap);

        if(slot>=header.numSlots)
        {
            // (no more slots in this block)

            // update block = right block of block
            block = header.rblock;
            // update slot = 0
            slot = 0;
            // (NOTE: if this is the last block, rblock would be -1. this would
            //        set block = -1 and fail the loop condition )
        }
        else if (slotMap[slot] == SLOT_UNOCCUPIED)
        { // (i.e slot-th entry in slotMap contains SLOT_UNOCCUPIED)

            // increment slot
            slot++;
        }
        else {
            // (the next occupied slot / record has been found)

            break;
        }
    }

    if (block == -1){
        // (a record was not found. all records exhausted)
        return E_NOTFOUND;
    }

    // declare nextRecId to store the RecId of the record found
    RecId nextRecId{block, slot};


    // set the search index to nextRecId using RelCacheTable::setSearchIndex
    RelCacheTable::setSearchIndex(relId, &nextRecId);

    /* Copy the record with record id (nextRecId) to the record buffer (record)
       For this Instantiate a RecBuffer class object by passing the recId and
       call the appropriate method to fetch the record
    */
    RecBuffer targetBlock(block);
    targetBlock.getRecord(record, slot);

    return SUCCESS;
}