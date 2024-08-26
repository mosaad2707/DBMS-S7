#include "Buffer/StaticBuffer.h"
#include "Cache/OpenRelTable.h"
#include "Disk_Class/Disk.h"
#include "FrontendInterface/FrontendInterface.h"
#include <iostream>

// Stage 1

// Function to write "hello" into a disk block and then read it back
void printhello()
{
  Disk disk_run; // Initialize a Disk object for reading and writing blocks

  unsigned char buffer[BLOCK_SIZE]; // Create a buffer to hold block data

  // Read block number 7000 into the buffer
  Disk::readBlock(buffer, 7000);

  // Define a message to write to the block
  char message[] = "hello";

  // Copy the message "hello" into the buffer at offset 20
  memcpy(buffer + 20, message, 6);

  // Write the modified buffer back to block number 7000
  Disk::writeBlock(buffer, 7000);

  unsigned char buffer2[BLOCK_SIZE]; // Create another buffer to read back the block

  char message2[6]; // Buffer to hold the read message

  // Read block number 7000 again into buffer2
  Disk::readBlock(buffer2, 7000);

  // Copy the message from buffer2 at offset 20 into message2
  memcpy(message2, buffer2 + 20, 6);

  // Print the message that was read back
  std::cout << message2;
}

// Function to print the content of a specified disk block
void printt_block_content(int block_num)
{
  Disk disk_run; // Initialize a Disk object

  unsigned char buffer[BLOCK_SIZE]; // Create a buffer to hold block data

  // Read the specified block number into the buffer
  Disk::readBlock(buffer, block_num);

  // Print the block number and its content byte by byte
  std::cout << "Block " << block_num << ": ";
  for (int j = 0; j < BLOCK_SIZE; j++)
  {
    std::cout << (int)buffer[j] << " "; // Print each byte as an integer
  }
  std::cout << std::endl; // End the line after printing the block content
}

// Stage 2

// Function to print relations and their attributes from catalogs
void print_relations_attributes()
{
  RecBuffer relCatBuffer(RELCAT_BLOCK);   // Initialize a RecBuffer for the relation catalog block
  RecBuffer attrCatBuffer(ATTRCAT_BLOCK); // Initialize a RecBuffer for the attribute catalog block

  HeadInfo relCatHeader;  // Declare a HeadInfo structure for the relation catalog header
  HeadInfo attrCatHeader; // Declare a HeadInfo structure for the attribute catalog header

  // Load the headers of both blocks into relCatHeader and attrCatHeader
  relCatBuffer.getHeader(&relCatHeader);
  attrCatBuffer.getHeader(&attrCatHeader);

  // Iterate over each entry in the relation catalog
  for (int i = 0; i < relCatHeader.numEntries; i++)
  {
    Attribute relCatRecord[RELCAT_NO_ATTRS]; // Declare an array to store the relation catalog record

    // Load the record at index i into relCatRecord
    relCatBuffer.getRecord(relCatRecord, i);

    // Print the name of the relation from the catalog
    // printf("Relation: %s\n", relCatRecord[RELCAT_REL_NAME_INDEX].sVal);

    // Initialize the attribute catalog block number to start reading
    int attrCatBlockNumber = ATTRCAT_BLOCK;

    // Loop to read through attribute catalog blocks linked by right block pointers
    while (attrCatBlockNumber != -1)
    {
      RecBuffer attrCatBuffer(attrCatBlockNumber); // Initialize a RecBuffer for the current attribute catalog block
      HeadInfo attrCatHeader;                      // Declare a HeadInfo structure for the attribute catalog header
      attrCatBuffer.getHeader(&attrCatHeader);     // Load the header into attrCatHeader
      attrCatBlockNumber = attrCatHeader.rblock;   // Update the block number to the next block

      // Iterate over each entry in the attribute catalog
      for (int j = 0; j < attrCatHeader.numEntries; j++)
      {
        Attribute attrCatRecord[ATTRCAT_NO_ATTRS]; // Declare an array to store the attribute catalog record

        // Load the record at index j into attrCatRecord
        attrCatBuffer.getRecord(attrCatRecord, j);

        // Check if the attribute belongs to the current relation
        if (strcmp(attrCatRecord[ATTRCAT_REL_NAME_INDEX].sVal, relCatRecord[RELCAT_REL_NAME_INDEX].sVal))
          continue; // If it doesn't match, skip to the next attribute

        // Determine the type of the attribute (NUM or STR)
        const char *attrType = (attrCatRecord[ATTRCAT_ATTR_TYPE_INDEX].nVal == NUMBER)
                                   ? "NUM"
                                   : "STR";

        // Print the attribute name and type
        printf("%s: %s\n", attrCatRecord[ATTRCAT_ATTR_NAME_INDEX].sVal, attrType);
      }
    }
    printf("\n"); // Print a newline to separate different relations
  }
}

// Stage 2 excercise 2
void update_student_schema()
{
  RecBuffer attrCatBuffer(ATTRCAT_BLOCK); // Initialize a RecBuffer for the attribute catalog block
  HeadInfo attrCatHeader;                 // Declare a HeadInfo structure for the attribute catalog header

  // Iterate through attribute catalog blocks to find the Student relation attributes
  int attrCatBlockNumber = ATTRCAT_BLOCK;
  while (attrCatBlockNumber != -1)
  {
    RecBuffer attrCatBuffer(attrCatBlockNumber); // Initialize a RecBuffer for the current attribute catalog block
    attrCatBuffer.getHeader(&attrCatHeader);     // Load the header into attrCatHeader
    attrCatBlockNumber = attrCatHeader.rblock;   // Update the block number to the next block

    // Iterate over each entry in the attribute catalog
    for (int i = 0; i < attrCatHeader.numEntries; i++)
    {
      Attribute attrCatRecord[ATTRCAT_NO_ATTRS]; // Declare an array to store the attribute catalog record
      int recordsize;
      // Load the record at index i into attrCatRecord
      attrCatBuffer.getRecord(attrCatRecord, i);

      // Check if the record belongs to the Student relation and has the "Class" attribute
      if (strcmp(attrCatRecord[ATTRCAT_REL_NAME_INDEX].sVal, "Students") == 0)
      {
        if (strcmp(attrCatRecord[ATTRCAT_ATTR_NAME_INDEX].sVal, "Class") == 0)
        {
          unsigned char buffer[BLOCK_SIZE];
          Disk::readBlock(buffer, ATTRCAT_BLOCK);
          int attrCount = attrCatHeader.numAttrs;  // Number of attributes in the record
          int slotCount =attrCatHeader.numSlots;  // Number of slots in the block
          int recordSize = attrCount * ATTR_SIZE;
          unsigned char *slotPointer = buffer + HEADER_SIZE + slotCount + (recordSize * i)+16;
          memcpy(slotPointer, "Batch", ATTR_SIZE);
          Disk::writeBlock(buffer, ATTRCAT_BLOCK);
          return;
        }
      }
    }
  }
}

// Main function
// int main(int argc, char *argv[])
// {
//   Disk disk_run; // Initialize a Disk object
//   StaticBuffer buffer;
//   OpenRelTable cache;
//   // Print the current relations and attributes
//   std::cout << "Before Schema Update:" << std::endl;
//   // print_relations_attributes();

//   // // Update the schema of the Student relation
//   // update_student_schema();

//   // Print the relations and attributes again to confirm the update
//   // std::cout << "After Schema Update:" << std::endl;
//   // print_relations_attributes();

// //   for (int i = 0; i <=2; i++) {
// //     RelCatEntry relCatBuf;
// //     RelCacheTable::getRelCatEntry(i, &relCatBuf);

// //     printf("Relation: %s\n", relCatBuf.relName);

// //     for (int j = 0; j < relCatBuf.numAttrs; j++) {
// //       AttrCatEntry attrCatBuf;
// //       AttrCacheTable::getAttrCatEntry(i, j, &attrCatBuf);
// //       const char* attrType = (attrCatBuf.attrType == NUMBER)
// //         ? "NUM"
// //         : "STR";
// //       printf("\t%s : %s\n", attrCatBuf.attrName, attrType);
// //     }
// //   }



// //   return 0; // Return success
// // }

int main(int argc, char *argv[]) {
  Disk disk_run;
  StaticBuffer buffer;
  OpenRelTable cache;

  return FrontendInterface::handleFrontend(argc, argv);
}