/**
 * @author See Contributors.txt for code contributors and overview of BadgerDB.
 *
 * @section LICENSE
 * Copyright (c) 2012 Database Group, Computer Sciences Department, University
 * of Wisconsin-Madison.
 */

#include "buffer.h"

#include <iostream>
#include <memory>

#include "exceptions/bad_buffer_exception.h"
#include "exceptions/buffer_exceeded_exception.h"
#include "exceptions/hash_not_found_exception.h"
#include "exceptions/page_not_pinned_exception.h"
#include "exceptions/page_pinned_exception.h"

namespace badgerdb {

constexpr int HASHTABLE_SZ(int bufs) { return ((int)(bufs * 1.2) & -2) + 1; }

//----------------------------------------
// Constructor of the class BufMgr
//----------------------------------------

BufMgr::BufMgr(std::uint32_t bufs)
    : numBufs(bufs),
      hashTable(HASHTABLE_SZ(bufs)),
      bufDescTable(bufs),
      bufPool(bufs) {
  for (FrameId i = 0; i < bufs; i++) {
    bufDescTable[i].frameNo = i;
    bufDescTable[i].valid = false;
  }

  clockHand = bufs - 1;
}

/**
   * Advance clock to next frame in the buffer pool
*/
void BufMgr::advanceClock() {
  	clockHand = (clockHand + 1) % numBufs;
}

/**
   * Allocate a free frame.
   *
   * @param frame   	Frame reference, frame ID of allocated frame returned
   * via this variable
   * @throws BufferExceededException If no such buffer is found which can be
   * allocated
  */
void badgerdb::BufMgr::allocBuf(FrameId & frame) 
{   
    uint32_t frame_count = 0;
    bool alloc = false;
    
    while(frame_count <= numBufs){
        if (bufDescTable[clockHand].valid == false){
            frame = bufDescTable[clockHand].frameNo;
            alloc = true;
            return;
        }
        if(bufDescTable[clockHand].valid == true){
        	if(bufDescTable[clockHand].refbit == true){
            	bufDescTable[clockHand].refbit = false;
           		advanceClock();
           		frame_count++;
           	}
           	else{
           		if(bufDescTable[clockHand].pinCnt == 0){
           			// if dirty bit it true, then write to disk
    				if(bufDescTable[clockHand].dirty == true){
        			   bufDescTable[clockHand].file.writePage(bufPool[clockHand]);
                 	   hashTable.remove(bufDescTable[clockHand].file, bufDescTable[clockHand].pageNo);
                 	   bufDescTable[clockHand].clear();
                 	   alloc = true;
                 	   frame = bufDescTable[clockHand].frameNo;
                 	   break;
    				 }
    				 // dirty = false
    				 else{
        				frame = bufDescTable[clockHand].frameNo;
        				alloc = true;
        				break;
   					 }
           		}
           		// pin count > 0
           		else{
           		    advanceClock();
           			frame_count++;
           		}
           	}
        }
    }
    if(alloc == false){
        throw BufferExceededException();
    }
}

/**
   * Reads the given page from the file into a frame and returns the pointer to
   * page. If the requested page is already present in the buffer pool pointer
   * to that frame is returned otherwise a new frame is allocated from the
   * buffer pool for reading the page.
   *
   * @param file   	File object
   * @param PageNo  Page number in the file to be read
   * @param page  	Reference to page pointer. Used to fetch the Page object
   * in which requested page from file is read in.
   */
void badgerdb::BufMgr::readPage(File& file, const PageId pageNo, Page*& page) {
    FrameId frameNo;
    try{
    	// if page not found then goes to catch part, else increments pin count
    	hashTable.lookup(file, pageNo, frameNo);
    	bufDescTable[frameNo].refbit = true;
    	bufDescTable[frameNo].pinCnt++;
    	bufStats.accesses++;
    	page = &(bufPool[frameNo]);  	
    }
    catch(HashNotFoundException& e){
    	    //call allocBuf() to allocate a buffer frame
    	try{
    		allocBuf(frameNo);
    		//calls the readPage method to read the page from disk into buffer pool
    		Page page_to_read = file.readPage(pageNo);
    		bufStats.diskreads++;
    		bufPool[frameNo] = page_to_read;
    		page = &bufPool[frameNo];
    		//inserts the page into the hash table
            hashTable.insert(file, pageNo, frameNo);
            //Sets the frame properly
            bufDescTable[frameNo].Set(file, pageNo);
          }
          catch(BufferExceededException e){
    		std::cerr << "BufferExceededException: No such buffer found!" << "\n";
    	}
    }
}


void badgerdb::BufMgr::unPinPage(File& file, const PageId pageNo, const bool dirty) {
    FrameId frameNo;
    try{
  		hashTable.lookup(file, pageNo, frameNo);
    	if (bufDescTable[frameNo].pinCnt == 0){
    		throw PageNotPinnedException(file.filename(), pageNo, frameNo);
    	}
    	bufDescTable[frameNo].pinCnt--;
    	if(dirty){
       	   bufDescTable[frameNo].dirty = true;
    	}
    }
    catch(HashNotFoundException e){
    	std::cerr << "Hash not found while unpinning page!" << "\n";
    }
  }

void badgerdb::BufMgr::allocPage(File& file, PageId& pageNo, Page*& page) {
	FrameId frameNo;
    allocBuf(frameNo);
    //create a new page to allocate
    Page page_to_allocate = file.allocatePage();
    pageNo = page_to_allocate.page_number();
    //allocates at the frame in the buffer
    bufPool[frameNo] = page_to_allocate;
    bufStats.accesses++;
    //page holds a pointer to the frame
    page = &bufPool[frameNo]; 
    //set the frame properly in the buffer
    bufDescTable[frameNo].Set(file, pageNo);
    //inserts the entry into the hash table`
    hashTable.insert(file, pageNo, frameNo);

}

void badgerdb::BufMgr::flushFile(File& file) {
        //Check bufDescTable for pages belong to file and any excpetion
        for (FrameId id = 0; id < numBufs; id++){
           if (bufDescTable[id].file == file){
                //Throw PagePinnedException if page is pinned
                if (bufDescTable[id].pinCnt != 0){
                    throw PagePinnedException("Page is pinned", bufDescTable[id].pageNo, id);
                }
                //Throw BadBufferException if an invalid page encountered.
                if (bufDescTable[id].valid == 0){
                    throw BadBufferException(id, bufDescTable[id].dirty, bufDescTable[id].valid, bufDescTable[id].refbit);
                }
                //If the page is dirty, call writePage() to write the page to disk and set the dirty bit to false
                if (bufDescTable[id].dirty != 0){
                    bufDescTable[id].file.writePage(bufPool[id]);
                    bufDescTable[id].dirty = 0;
                }
                //Remove the page from the hashtable
                hashTable.remove(file, bufDescTable[id].pageNo);
                //Call the clear() method of BufDesc for the page frame.
                bufDescTable[id].clear();
            }
        }
    }

void badgerdb::BufMgr::disposePage(File& file, const PageId PageNo) {
	FrameId frame;
	try
    {
        hashTable.lookup(file, PageNo, frame);
        bufDescTable[frame].clear();
        //the entry is deleted from the hash Table
        hashTable.remove(file, PageNo);
    }
    // Does nothing in this case
    catch (HashNotFoundException e){
    }
    //deletes the page from file
    file.deletePage(PageNo);  
}

void badgerdb::BufMgr::printSelf(void) {
  int validFrames = 0;

  for (FrameId i = 0; i < numBufs; i++) {
    std::cout << "FrameNo:" << i << " ";
    bufDescTable[i].Print();

    if (bufDescTable[i].valid) validFrames++;
  }

  std::cout << "Total Number of Valid Frames:" << validFrames << "\n";
}

}  // namespace badgerdb
