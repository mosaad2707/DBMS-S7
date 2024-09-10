#include "Algebra.h"

#include <cstring>
#include <stdlib.h>
#include<stdio.h>

// Function to check if a string represents a number
bool isNumber(char* str) {
    int len;
    float ignore;
    
    // sscanf checks if the string can be parsed as a float
    // %f reads a floating-point number, %n records the number of characters processed
    int ret = sscanf(str, " %f %n", &ignore, &len);

    // The string is a valid number if sscanf returns 1 (successful conversion) and
    // len matches the string length (ensures the whole string was a number)
    return ret == 1 && len == strlen(str);
}

/* Function to select all records from a source relation (table) that satisfy a given condition.
 * Parameters:
 * - srcRel: The name of the source relation from which to select records.
 * - targetRel: The name of the target relation to select into (currently ignored).
 * - attr: The name of the attribute to apply the condition on.
 * - op: The operator used for the condition (e.g., EQ for equality).
 * - strVal: The value to compare against, represented as a string.
 */
//Hashed in Stage 9
// int Algebra::select(char srcRel[ATTR_SIZE], char targetRel[ATTR_SIZE], char attr[ATTR_SIZE], int op, char strVal[ATTR_SIZE]) {
//     // Get the relation ID of the source relation
//     int srcRelId = OpenRelTable::getRelId(srcRel);
//     // printf("srcRelId: %d\n", srcRelId);
//     // Return error code if the relation is not open
//     if (srcRelId == E_RELNOTOPEN) 
//     {
//         return srcRelId;    
//     }

//     // Fetch the attribute catalog entry for the specified attribute in the source relation
//     AttrCatEntry attrCatEntry;
//     int ret = AttrCacheTable::getAttrCatEntry(srcRelId, attr, &attrCatEntry);

//     // Return error code if the attribute does not exist
//     if (ret == E_ATTRNOTEXIST)
//         return ret;

//     // Determine the type of the attribute (e.g., NUMBER or STRING)
//     int type = attrCatEntry.attrType;
//     Attribute attrVal;

//     // Convert the string value to the appropriate attribute type
//     if (type == NUMBER) {
//         // Check if the string value is a valid number
//         if (isNumber(strVal))
//             attrVal.nVal = atof(strVal); // Convert the string to a floating-point number
//         else 
//             return E_ATTRTYPEMISMATCH; // Return error if the string is not a valid number
//     }
//     else if (type == STRING) {
//         strcpy(attrVal.sVal, strVal); // Copy the string value
//     }

//     // Reset the search index for the source relation, starting a fresh search
//     RelCacheTable::resetSearchIndex(srcRelId);

//     // Retrieve the catalog entry for the relation
//     RelCatEntry relCatEntry;
//     RelCacheTable::getRelCatEntry(srcRelId, &relCatEntry);

//     // Print the attribute names as table headers
//     printf("|");
//     for (int i = 0; i < relCatEntry.numAttrs; i++) {
//         AttrCatEntry attrCatEntry;
//         AttrCacheTable::getAttrCatEntry(srcRelId, i, &attrCatEntry);

//         printf(" %s\t|", attrCatEntry.attrName);
//     }
//     printf("\n");

//     // Perform a linear search to find records matching the condition
//     while (true) {
//         // printf("srcRelId: %d\n", srcRelId);
//         // printf("attr: %s\n", attr);
//         // printf("attrVal: %s\n", attrVal.sVal);
//         // printf("op: %d\n", op);

//         RecId searchRes = BlockAccess::linearSearch(srcRelId, attr, attrVal, op);
//         // printf("searchRes.block: %d\n", searchRes.block);

//         // Check if a valid record is found
//         if (searchRes.block != -1 && searchRes.slot != -1) {
//             // Retrieve the record from the found block and slot
//             Attribute record[relCatEntry.numAttrs];

//             RecBuffer recBuf(searchRes.block);
//             recBuf.getRecord(record, searchRes.slot);

//             // Print the record's attribute values
//             printf("|");
//             for (int i = 0; i < relCatEntry.numAttrs; i++) {
//                 AttrCatEntry attrCatEntry;
//                 AttrCacheTable::getAttrCatEntry(srcRelId, i, &attrCatEntry);
                
//                 // Print the value based on its type (NUMBER or STRING)
//                 (attrCatEntry.attrType == NUMBER)
//                     ? printf(" %f\t|", record[i].nVal)
//                     : printf(" %s\t|", record[i].sVal);
//             }
//             printf("\n");
//         } else {
//             // No more matching records found, exit the loop
//             break;
//         }
//     }

//     return SUCCESS; // Return success code
// }

//Stage 7
int Algebra::insert(char relName[ATTR_SIZE], int nAttrs, char record[][ATTR_SIZE]) {
    // If relName is equal to "RELATIONCAT" or "ATTRIBUTECAT", return E_NOTPERMITTED
    if (
        strcmp(relName, (char*)RELCAT_RELNAME) == 0 ||
        strcmp(relName, (char*)ATTRCAT_RELNAME) == 0
    ) {
        return E_NOTPERMITTED;
    }

    // Get the relation's rel-id using OpenRelTable::getRelId() method
    int relId = OpenRelTable::getRelId(relName);

    // If the relation is not open in the open relation table, return E_RELNOTOPEN
    // (Check if the value returned from getRelId function call equals E_RELNOTOPEN)
    if (relId == E_RELNOTOPEN)
        return E_RELNOTOPEN;

    // Get the relation catalog entry from the relation cache
    // (Use RelCacheTable::getRelCatEntry() of the Cache Layer)
    RelCatEntry relCatBuf;
    RelCacheTable::getRelCatEntry(relId, &relCatBuf);

    // If relCatEntry.numAttrs != numberOfAttributes in relation, return E_NATTRMISMATCH
    if (relCatBuf.numAttrs != nAttrs)
        return E_NATTRMISMATCH;

    // Declare an array of type union Attribute to hold the record values
    Attribute recordValues[nAttrs];

    // Convert the 2D char array of record values to the Attribute array recordValues
    for (int i = 0; i < nAttrs; i++) {
        // Get the attr-cat entry for the i'th attribute from the attr-cache
        // (Use AttrCacheTable::getAttrCatEntry())
        AttrCatEntry attrCatBuf;
        AttrCacheTable::getAttrCatEntry(relId, i, &attrCatBuf);

        // Determine the type of the attribute
        if (attrCatBuf.attrType == NUMBER) {
            // If the char array record[i] can be converted to a number
            // (Check this using isNumber() function)
            if (!isNumber(record[i]))
                return E_ATTRTYPEMISMATCH;

            // Convert the char array to a number and store it in recordValues[i].nVal using atof()
            recordValues[i].nVal = atof(record[i]);
        } else {
            // Copy record[i] to recordValues[i].sVal for STRING type attributes
            strcpy(recordValues[i].sVal, record[i]);
        } 
    }
    // printf("Inserting record into relation %s\n", relName);

    // Insert the record by calling BlockAccess::insert() function
    // Let retVal denote the return value of the insert call
    int retval = BlockAccess::insert(relId, recordValues);
    return retval;
}

//Stage 9
int Algebra::select(char srcRel[ATTR_SIZE], char targetRel[ATTR_SIZE], char attr[ATTR_SIZE], int op, char strVal[ATTR_SIZE]) {
    // Get the srcRel's rel-id using OpenRelTable::getRelId()
    int srcRelId = OpenRelTable::getRelId(srcRel);

    // If srcRel is not open in the open relation table, return E_RELNOTOPEN
    if (srcRelId == E_RELNOTOPEN) 
        return srcRelId;

    // Get the attribute catalog entry for attr using AttrCacheTable::getAttrCatEntry()
    AttrCatEntry attrCatEntry;
    int ret = AttrCacheTable::getAttrCatEntry(srcRelId, attr, &attrCatEntry);

    // If getAttrCatEntry() call fails, return E_ATTRNOTEXIST
    if (ret == E_ATTRNOTEXIST)
        return ret;

    /*** Convert strVal to an attribute of data type NUMBER or STRING ***/
    Attribute attrVal;
    int type = attrCatEntry.attrType;

    if (type == NUMBER) {
        // If the input argument strVal can be converted to a number
        // Check this using isNumber() function
        if (isNumber(strVal)) {
            // Convert strVal to double and store it at attrVal.nVal using atof()
            attrVal.nVal = atof(strVal);
        } else {
            // If strVal cannot be converted to a number, return E_ATTRTYPEMISMATCH
            return E_ATTRTYPEMISMATCH;
        }
    } else if (type == STRING) {
        // Copy strVal to attrVal.sVal
        strcpy(attrVal.sVal, strVal);
    }

    /*** Creating and opening the target relation ***/
    // Get RelCatEntry of srcRel using RelCacheTable::getRelCatEntry()
    RelCatEntry relCatEntry;
    RelCacheTable::getRelCatEntry(srcRelId, &relCatEntry);

    // Number of attributes present in src relation
    int src_nAttrs = relCatEntry.numAttrs;

    // Declare arrays to store attribute names and types
    char attrNames[src_nAttrs][ATTR_SIZE];
    int attrTypes[src_nAttrs];

    // Iterate through attributes and fill the attrNames and attrTypes arrays
    for (int i = 0; i < src_nAttrs; i++) {
        AttrCatEntry attrCatEntry;
        AttrCacheTable::getAttrCatEntry(srcRelId, i, &attrCatEntry);
        
        // Fill attribute names and types
        strcpy(attrNames[i], attrCatEntry.attrName);
        attrTypes[i] = attrCatEntry.attrType;
    }

    // Create the target relation using Schema::createRel()
    ret = Schema::createRel(targetRel, src_nAttrs, attrNames, attrTypes);
    if (ret != SUCCESS)
        return ret;

    // Open the newly created target relation
    int targetRelId = OpenRelTable::openRel(targetRel);

    // If opening fails, delete the target relation and return the error value
    if (targetRelId < 0 || targetRelId >= MAX_OPEN) {
        Schema::deleteRel(targetRel);
        return targetRelId;
    }

    /*** Selecting and inserting records into the target relation ***/
    // Reset the search index for srcRel to start from the first record
    RelCacheTable::resetSearchIndex(srcRelId);

    // Reset the search index in the attribute cache for the select condition attribute
    AttrCacheTable::resetSearchIndex(srcRelId, attr);

    // Declare a record array to store the current record being processed
    Attribute record[src_nAttrs];

    // Search and insert records that satisfy the condition
    while (BlockAccess::search(srcRelId, record, attr, attrVal, op) == SUCCESS) {
        // Insert the record into the target relation
        ret = BlockAccess::insert(targetRelId, record);
        
        // If insertion fails, close and delete the target relation, then return the error
        if (ret != SUCCESS) {
            OpenRelTable::closeRel(targetRelId);
            Schema::deleteRel(targetRel);
            return ret;
        }
    }

    // Close the target relation
    OpenRelTable::closeRel(targetRelId);

    // Return SUCCESS
    return SUCCESS;
}

