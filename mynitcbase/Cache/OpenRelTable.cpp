#include "OpenRelTable.h"
#include <iostream>
#include <cstring>

void clearList(AttrCacheEntry *head)
{
    for (AttrCacheEntry *it = head, *next; it != nullptr; it = next)
    {
        next = it->next;
        free(it);
    }
}

AttrCacheEntry *createAttrCacheEntryList(int size)
{
    AttrCacheEntry *head = nullptr, *curr = nullptr;
    head = curr = (AttrCacheEntry *)malloc(sizeof(AttrCacheEntry));
    size--;
    while (size--)
    {
        curr->next = (AttrCacheEntry *)malloc(sizeof(AttrCacheEntry));
        curr = curr->next;
    }
    curr->next = nullptr;

    return head;
}
OpenRelTableMetaInfo OpenRelTable::tableMetaInfo[MAX_OPEN];
OpenRelTable::OpenRelTable() {
    // Initialize each entry in the relation and attribute cache tables to nullptr 
    // and mark all entries in the tableMetaInfo array as free.
    for (int i = 0; i < MAX_OPEN; i++) {
        RelCacheTable::relCache[i] = nullptr;  // Initialize relation cache entry to nullptr
        AttrCacheTable::attrCache[i] = nullptr; // Initialize attribute cache entry to nullptr
        OpenRelTable::tableMetaInfo[i].free = true; // Mark tableMetaInfo entry as free
    }

    // Initialize the relation catalog block and temporary storage for relation catalog records
    RecBuffer relCatBlock(RELCAT_BLOCK);
    Attribute relCatRecord[RELCAT_NO_ATTRS];
    
    // Loop through each entry in the relation catalog (RELCAT_RELID to ATTRCAT_RELID)
    // and populate the relation cache with these entries.
    for (int i = RELCAT_RELID; i <= ATTRCAT_RELID; i++) {
        relCatBlock.getRecord(relCatRecord, i); // Fetch the i-th record from the relation catalog

        struct RelCacheEntry relCacheEntry; // Temporary storage for relation cache entry
        RelCacheTable::recordToRelCatEntry(relCatRecord, &relCacheEntry.relCatEntry); // Convert record to cache entry
        relCacheEntry.recId.block = RELCAT_BLOCK; // Assign block number to recId
        relCacheEntry.recId.slot = i; // Assign slot number to recId

        // Allocate memory for the cache entry and store it in the relation cache
        RelCacheTable::relCache[i] = (struct RelCacheEntry*) malloc(sizeof(RelCacheEntry));
        *(RelCacheTable::relCache[i]) = relCacheEntry; // Copy the constructed entry to the cache
        tableMetaInfo[i].free = false; // Mark this tableMetaInfo entry as occupied
        memcpy(tableMetaInfo[i].relName, relCacheEntry.relCatEntry.relName, ATTR_SIZE); // Store the relation name
    }

    // Initialize the attribute catalog block and temporary storage for attribute catalog records
    RecBuffer attrCatBlock(ATTRCAT_BLOCK);
    Attribute attrCatRecord[ATTRCAT_NO_ATTRS];
    
    // Create a linked list for storing attribute cache entries for the relation catalog attributes
    auto relCatListHead = createAttrCacheEntryList(RELCAT_NO_ATTRS);
    auto attrCacheEntry = relCatListHead;

    // Populate the attribute cache with entries corresponding to relation catalog attributes
    for (int i = 0; i < RELCAT_NO_ATTRS; i++) {
        attrCatBlock.getRecord(attrCatRecord, i); // Fetch the i-th record from the attribute catalog

        // Convert record to attribute cache entry and store in the linked list
        AttrCacheTable::recordToAttrCatEntry(attrCatRecord, &(attrCacheEntry->attrCatEntry));
        (attrCacheEntry->recId).block = ATTRCAT_BLOCK; // Assign block number to recId
        (attrCacheEntry->recId).slot = i; // Assign slot number to recId
        
        attrCacheEntry = attrCacheEntry->next; // Move to the next entry in the list
    }
    AttrCacheTable::attrCache[RELCAT_RELID] = relCatListHead; // Store the head of the list in the attribute cache

    // Create a linked list for storing attribute cache entries for the attribute catalog attributes
    auto attrCatListHead = createAttrCacheEntryList(ATTRCAT_NO_ATTRS);
    attrCacheEntry = attrCatListHead;
    
    // Populate the attribute cache with entries corresponding to attribute catalog attributes
    for (int i = RELCAT_NO_ATTRS; i < RELCAT_NO_ATTRS + ATTRCAT_NO_ATTRS; i++) {
        attrCatBlock.getRecord(attrCatRecord, i); // Fetch the i-th record from the attribute catalog

        // Convert record to attribute cache entry and store in the linked list
        AttrCacheTable::recordToAttrCatEntry(attrCatRecord, &(attrCacheEntry->attrCatEntry));
        (attrCacheEntry->recId).block = ATTRCAT_BLOCK; // Assign block number to recId
        (attrCacheEntry->recId).slot = i; // Assign slot number to recId

        attrCacheEntry = attrCacheEntry->next; // Move to the next entry in the list
    }
    AttrCacheTable::attrCache[ATTRCAT_RELID] = attrCatListHead; // Store the head of the list in the attribute cache
}
//Commented at stage 8
// OpenRelTable::~OpenRelTable() {

//     // Close all open relations, starting from index 2 (assuming index 0 and 1 are reserved)
//     for (int i = 2; i < MAX_OPEN; i++) {
//         if (!tableMetaInfo[i].free)
//             OpenRelTable::closeRel(i); // Close the relation at index i if it is open
//     }

//     // Free all allocated memory and reset the relation and attribute caches
//     for (int i = 0; i < MAX_OPEN; i++) {
//         free(RelCacheTable::relCache[i]); // Free memory allocated for relation cache entry
//         clearList(AttrCacheTable::attrCache[i]); // Free memory for the linked list in attribute cache

//         RelCacheTable::relCache[i] = nullptr; // Reset relation cache entry to nullptr
//         AttrCacheTable::attrCache[i] = nullptr; // Reset attribute cache entry to nullptr
//     }
// }


/* This function will open a relation having name `relName`.
Since we are currently only working with the relation and attribute catalog, we
will just hardcode it. In subsequent stages, we will loop through all the relations
and open the appropriate one.
*/
int OpenRelTable::getRelId(char relName[ATTR_SIZE]) {

  /* traverse through the tableMetaInfo array,
    find the entry in the Open Relation Table corresponding to relName.*/
    for(int i = 0; i < MAX_OPEN; i++) {
        // printf("tableMetaInfo[i].relName: %s\n", tableMetaInfo[i].relName);
        if(tableMetaInfo[i].free==false && strcmp(tableMetaInfo[i].relName, relName) == 0) {
            return i;
        }
    }

  // if found return the relation id, else indicate that the relation do not
  // have an entry in the Open Relation Table.
    return E_RELNOTOPEN;
}

int OpenRelTable::getFreeOpenRelTableEntry() {

  /* traverse through the tableMetaInfo array,
    find a free entry in the Open Relation Table.*/
    for(int i = 2; i < MAX_OPEN; i++) {
        if(tableMetaInfo[i].free) {
            return i;
        }
    }

  // if found return the relation id, else return E_CACHEFULL.
    return E_CACHEFULL;
}

int OpenRelTable::closeRel(int relId) {
    if (relId == RELCAT_RELID || relId == ATTRCAT_RELID)
        return E_NOTPERMITTED;

    if (relId < 0 || relId >= MAX_OPEN)
        return E_OUTOFBOUND;

    if (OpenRelTable::tableMetaInfo[relId].free)
        return E_RELNOTOPEN;

    if (RelCacheTable::relCache[relId]->dirty) {
        Attribute record[RELCAT_NO_ATTRS];

        RelCacheTable::relCatEntryToRecord(&RelCacheTable::relCache[relId]->relCatEntry, record);

        RecBuffer relCatBuffer(RelCacheTable::relCache[relId]->recId.block);

        relCatBuffer.setRecord(record, RelCacheTable::relCache[relId]->recId.slot);
    }

    OpenRelTable::tableMetaInfo[relId].free = true;
    free(RelCacheTable::relCache[relId]);
    clearList(AttrCacheTable::attrCache[relId]);

    RelCacheTable::relCache[relId] = nullptr;
    AttrCacheTable::attrCache[relId] = nullptr;

    int relIdfinal=OpenRelTable::getRelId(OpenRelTable::tableMetaInfo[relId].relName);
    printf("relIdfinal: %d\n", relIdfinal);

    return SUCCESS;


}

int OpenRelTable::openRel(char relName[ATTR_SIZE]) {
    // Let relId be used to store the free slot.
    int relId = getRelId(relName);

    // Check if the relation is already open (checked using OpenRelTable::getRelId())
    if (relId != E_RELNOTOPEN) {
        // Return the existing relation ID
        return relId;
    }

    // Find a free slot in the Open Relation Table using OpenRelTable::getFreeOpenRelTableEntry().
    int fslot = getFreeOpenRelTableEntry();

    // If no free slot is found, return E_CACHEFULL indicating the cache is full.
    if (fslot == E_CACHEFULL) {
        return E_CACHEFULL;
    }

    /****** Setting up Relation Cache entry for the relation ******/

    // Create an Attribute object to hold the relation name and copy relName into it.
    Attribute relNameAttribute;
    memcpy(relNameAttribute.sVal, relName, ATTR_SIZE);

    // Reset the search index for the relation catalog (RELCAT_RELID) before calling linearSearch().
    RelCacheTable::resetSearchIndex(RELCAT_RELID);

    // Search for the entry with relation name, relName, in the Relation Catalog using BlockAccess::linearSearch().
    RecId relcatRecId = BlockAccess::linearSearch(RELCAT_RELID, (char*)RELCAT_ATTR_RELNAME, relNameAttribute, EQ);

    // If the relation is not found in the catalog, return E_RELNOTEXIST.
    if (relcatRecId.block == -1 && relcatRecId.slot == -1) {
        return E_RELNOTEXIST;
    }

    // Retrieve the record from the block using the record ID (relcatRecId).
    RecBuffer recBuffer(relcatRecId.block);
    Attribute record[RELCAT_NO_ATTRS];
    recBuffer.getRecord(record, relcatRecId.slot);

    // Create a RelCatEntry structure to store the relation catalog entry.
    RelCatEntry relCatEntry;
    RelCacheTable::recordToRelCatEntry(record, &relCatEntry);

    // Allocate memory for a new RelCacheEntry and set up the cache entry in the free slot.
    RelCacheTable::relCache[fslot] = (RelCacheEntry*) malloc(sizeof(RelCacheEntry));
    RelCacheTable::relCache[fslot]->recId = relcatRecId;
    RelCacheTable::relCache[fslot]->relCatEntry = relCatEntry;

    // Initialize a list to cache attribute entries, matching the number of attributes in the relation.
    int numAttrs = relCatEntry.numAttrs;
    AttrCacheEntry* listHead = createAttrCacheEntryList(numAttrs);
    AttrCacheEntry* node = listHead;

    // Reset the search index for the attribute catalog (ATTRCAT_RELID) before performing the search.
    RelCacheTable::resetSearchIndex(ATTRCAT_RELID);

    // Loop to search for each attribute of the relation in the Attribute Catalog and cache them.
    while (true) {
        // Search for attributes associated with the relation name.
        RecId searchRes = BlockAccess::linearSearch(ATTRCAT_RELID, (char*)ATTRCAT_ATTR_RELNAME, relNameAttribute, EQ);

        // If a valid attribute is found, retrieve and cache it.
        if (searchRes.block != -1 && searchRes.slot != -1) {
            Attribute attrcatRecord[ATTRCAT_NO_ATTRS];
            RecBuffer attrRecBuffer(searchRes.block);
            attrRecBuffer.getRecord(attrcatRecord, searchRes.slot);

            AttrCatEntry attrCatEntry;
            AttrCacheTable::recordToAttrCatEntry(attrcatRecord, &attrCatEntry);

            node->recId = searchRes;
            node->attrCatEntry = attrCatEntry;
            node = node->next;
        } else {
            // Break out of the loop when no more attributes are found.
            break;
        }
    }

    // Link the cached attribute list to the corresponding entry in the Attribute Cache Table.
    AttrCacheTable::attrCache[fslot] = listHead;

    // Mark the slot as occupied and set the relation name in the metadata information table.
    OpenRelTable::tableMetaInfo[fslot].free = false;
    memcpy(OpenRelTable::tableMetaInfo[fslot].relName, relCatEntry.relName, ATTR_SIZE);

    // Return the ID of the free slot where the relation was opened.
    return fslot;
}

//Stage 8
OpenRelTable::~OpenRelTable() {

    for(int i = 0; i < MAX_OPEN; i++) {
        if(!tableMetaInfo[i].free) {
            closeRel(i);
        }
    }
    /**** Closing the catalog relations in the relation cache ****/

    //releasing the relation cache entry of the attribute catalog

    if (RelCacheTable::relCache[ATTRCAT_RELID]->dirty) {

        /* Get the Relation Catalog entry from RelCacheTable::relCache
        Then convert it to a record using RelCacheTable::relCatEntryToRecord(). */
        RelCatEntry relCatBuffer;
		RelCacheTable::getRelCatEntry(ATTRCAT_RELID, &relCatBuffer);
		Attribute relCatRecord [RELCAT_NO_ATTRS];
		RelCacheTable::relCatEntryToRecord(&relCatBuffer, relCatRecord);

		RecId recId = RelCacheTable::relCache[ATTRCAT_RELID]->recId;


        // declaring an object of RecBuffer class to write back to the buffer
        RecBuffer relCatBlock(recId.block);

        // Write back to the buffer using relCatBlock.setRecord() with recId.slot
        relCatBlock.setRecord(relCatRecord, recId.slot);
    }
    // free the memory dynamically allocated to this RelCacheEntry
    free(RelCacheTable::relCache[ATTRCAT_RELID]);



    //releasing the relation cache entry of the relation catalog

    if (RelCacheTable::relCache[RELCAT_RELID]->dirty)
	{
        /* Get the Relation Catalog entry from RelCacheTable::relCache
        Then convert it to a record using RelCacheTable::relCatEntryToRecord(). */
		RelCatEntry relCatBuffer;
		RelCacheTable::getRelCatEntry(RELCAT_RELID, &relCatBuffer);

		Attribute relCatRecord [RELCAT_NO_ATTRS];
		RelCacheTable::relCatEntryToRecord(&relCatBuffer, relCatRecord);

		RecId recId = RelCacheTable::relCache[RELCAT_RELID]->recId;

        // declaring an object of RecBuffer class to write back to the buffer
        RecBuffer relCatBlock(recId.block);

        // Write back to the buffer using relCatBlock.setRecord() with recId.slot
		relCatBlock.setRecord(relCatRecord, recId.slot);
    }
    // free the memory dynamically allocated for this RelCacheEntry
	free(RelCacheTable::relCache[RELCAT_RELID]);


    // free the memory allocated for the attribute cache entries of the
    clearList(AttrCacheTable::attrCache[RELCAT_RELID]);
    // relation catalog and the attribute catalog
    clearList(AttrCacheTable::attrCache[ATTRCAT_RELID]);
}