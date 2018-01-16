#include "inode_manager.h"
#include <ctime>

// disk layer -----------------------------------------

disk::disk()
{
  bzero(blocks, sizeof(blocks));
}

void
disk::read_block(blockid_t id, char *buf)
{
  /*
   *your lab1 code goes here.
   *if id is smaller than 0 or larger than BLOCK_NUM 
   *or buf is null, just return.
   *put the content of target block into buf.
   *hint: use memcpy
  */
  if (id < 0 || id >= BLOCK_NUM || !buf) {
    return;
  }

  memcpy(buf, blocks[id], BLOCK_SIZE);
  return;
}

void
disk::write_block(blockid_t id, const char *buf)
{
  /*
   *your lab1 code goes here.
   *hint: just like read_block
  */
  if (id < 0 || id >= BLOCK_NUM || !buf) {
    return;
  }

  /* 
   * Attention: not sizeof(buf) but BLOCK_SIZE
   * Because block is the basic unit of read and write
   */
  memcpy(blocks[id], buf, BLOCK_SIZE);
  return;
}

// block layer -----------------------------------------

// Allocate a free disk block.
blockid_t
block_manager::alloc_block()
{
  /*
   * your lab1 code goes here.
   * note: you should mark the corresponding bit in block bitmap when alloc.
   * you need to think about which block you can start to be allocated.

   *hint: use macro IBLOCK and BBLOCK.
          use bit operation.
          remind yourself of the layout of disk.
   */

  char buf[BLOCK_SIZE];
  char mask;
  
  /* Find the first empty bit in bitmap and alloc this block */
  blockid_t id;
  uint32_t offset;
  uint32_t num;
  /* Check every bitmap */
  for (id = 0; id < BLOCK_NUM; id += BPB) {
    read_block(BBLOCK(id), buf);
    /* Check every bit in bitmap */
    for (offset = 0; offset < BPB; offset++) {
      mask = 1 << (offset % 8);
      /* Find the empty bit (0) */
      num = buf[offset/8];
      if ((num & mask) == 0) {
        buf[offset/8] = num | mask;
        write_block(BBLOCK(id), buf);
        return id + offset;
      }
    }
  }

  return 0;
}

void
block_manager::free_block(uint32_t id)
{
  /* 
   * your lab1 code goes here.
   * note: you should unmark the corresponding bit in the block bitmap when free.
   */

  /* Protect superBlock & bitmap */
  if (id <= BBLOCK(BLOCK_NUM)) {
    return;
  }

  char buf[BLOCK_SIZE];
  read_block(BBLOCK(id), buf);

  uint32_t offset = id % BPB;
  uint32_t mask = ~(1 << (offset % 8));
  buf[offset/8] = buf[offset/8] & mask;
  write_block(BBLOCK(id), buf);
  return;
}

// The layout of disk should be like this:
// |<-sb->|<-free block bitmap->|<-inode table->|<-data->|
block_manager::block_manager()
{
  d = new disk();

  // format the disk
  sb.size = BLOCK_SIZE * BLOCK_NUM;
  sb.nblocks = BLOCK_NUM;
  sb.ninodes = INODE_NUM;

  /* 
   * Something hard to debug will happen 
   * if l didn't initial the disk
   */

  /* Alloc boot block */ 
  alloc_block();

  /* Alloc super block */ 
  alloc_block();

  /* Alloc bitmap */ 
  uint32_t i;
  for (i = 0; i < BLOCK_NUM / BPB; i++) {
    alloc_block();
  }

  /* Alloc inode table */ 
  for (i = 0; i < INODE_NUM / IPB; i++) {
    alloc_block();
  }
}

void
block_manager::read_block(uint32_t id, char *buf)
{
  d->read_block(id, buf);
}

void
block_manager::write_block(uint32_t id, const char *buf)
{
  d->write_block(id, buf);
}

// inode layer -----------------------------------------

inode_manager::inode_manager()
{
  bm = new block_manager();
  uint32_t root_dir = alloc_inode(extent_protocol::T_DIR);
  if (root_dir != 1) {
    printf("\tim: error! alloc first inode %d, should be 1\n", root_dir);
    exit(0);
  }
}

/* Create a new file.
 * Return its inum. */
uint32_t
inode_manager::alloc_inode(uint32_t type)
{
  /* 
   * your lab1 code goes here.
   * note: the normal inode block should begin from the 2nd inode block.
   * the 1st is used for root_dir, see inode_manager::inode_manager().
    
   * if you get some heap memory, do not forget to free it.
   */

  char buf[BLOCK_SIZE];

  uint32_t inodeNum;
  /* Check every inodeBlock */
  for (inodeNum = 1; inodeNum < bm->sb.ninodes; inodeNum++) {
    if ((inodeNum - 1) % IPB == 0) {
      bm->read_block(IBLOCK(inodeNum, bm->sb.nblocks), buf);
    } 
    /* Check every inode in block */
    inode_t *inode = (inode_t *)buf + (inodeNum - 1) % IPB;
    /* Find an empty inode */
    if (inode->type == 0) {
      inode->type = type;
      inode->ctime = std::time(0);
      bm->write_block(IBLOCK(inodeNum, bm->sb.nblocks), buf);
      return inodeNum;
    }
  }

  return 1;
}

void
inode_manager::free_inode(uint32_t inum)
{
  /* 
   * your lab1 code goes here.
   * note: you need to check if the inode is already a freed one;
   * if not, clear it, and remember to write back to disk.
   * do not forget to free memory if necessary.
   */

  char buf[BLOCK_SIZE];
  bm->read_block(IBLOCK(inum, bm->sb.nblocks), buf);
  inode_t *inode = (inode_t *)buf + (inum - 1) % IPB;

  /* Check if the inode is already a freed one */
  if (inode->type == 0) {
    return;
  } else {
    inode->type = 0;
    bm->write_block(IBLOCK(inum, bm->sb.nblocks), buf);
  }

  return;
}


/* Return an inode structure by inum, NULL otherwise.
 * Caller should release the memory. */
struct inode* 
inode_manager::get_inode(uint32_t inum)
{
  struct inode *ino, *ino_disk;
  char buf[BLOCK_SIZE];

  printf("\tim: get_inode %d\n", inum);

  if (inum < 0 || inum >= INODE_NUM) {
    printf("\tim: inum out of range\n");
    return NULL;
  }

  bm->read_block(IBLOCK(inum, bm->sb.nblocks), buf);
  // printf("%s:%d\n", __FILE__, __LINE__);

  ino_disk = (struct inode*)buf + inum%IPB;
  if (ino_disk->type == 0) {
    printf("\tim: inode not exist\n");
    return NULL;
  }

  ino = (struct inode*)malloc(sizeof(struct inode));
  *ino = *ino_disk;

  return ino;
}

void
inode_manager::put_inode(uint32_t inum, struct inode *ino)
{
  char buf[BLOCK_SIZE];
  struct inode *ino_disk;

  printf("\tim: put_inode %d\n", inum);
  if (ino == NULL)
    return;

  bm->read_block(IBLOCK(inum, bm->sb.nblocks), buf);
  ino_disk = (struct inode*)buf + inum%IPB;
  *ino_disk = *ino;
  bm->write_block(IBLOCK(inum, bm->sb.nblocks), buf);
}

#define MIN(a,b) ((a)<(b) ? (a) : (b))
#define MAX(a,b) ((a)>(b) ? (a) : (b))

/* Get all the data of a file by inum. 
 * Return alloced data, should be freed by caller. */
void
inode_manager::read_file(uint32_t inum, char **buf_out, int *size)
{
  /*
   * your lab1 code goes here.
   * note: read blocks related to inode number inum,
   * and copy them to buf_out
   */

  /* Get inode by inum */
  inode_t *inode = get_inode(inum);

  if (!inode) {
    return;
  }

  /* Get size of file and malloc a array to store data */
  unsigned int fileSize = inode->size;
  char *res = (char *)malloc(fileSize);

  /* Read NDIRECT data */
  char buf[BLOCK_SIZE];
  uint32_t blockNum;
  unsigned int nowSize = 0;

  for (blockNum = 0; blockNum < NDIRECT && nowSize < fileSize; 
    blockNum++) {
    /* Not last one */
    if (nowSize + BLOCK_SIZE < fileSize) {
      bm->read_block(inode->blocks[blockNum], res + nowSize);
      nowSize += BLOCK_SIZE;
    }

    /* Last one */
    else {
      int len = fileSize - nowSize;
      bm->read_block(inode->blocks[blockNum], buf);
      memcpy(res + nowSize, buf, len);
      nowSize += len;
    }
  }

  /* Judge whether NINDIRECT, if NINDIRECT, read it */
  if (nowSize < fileSize) {
    /* Read NINDIRECT block */
    blockid_t indirectBlock[NINDIRECT];
    bm->read_block(inode->blocks[NDIRECT], (char *)indirectBlock);
    

    /* Read the data */ 
    for (blockNum = 0; blockNum < NINDIRECT && nowSize < fileSize; 
      blockNum++) {
      /* Not last one */
      if (nowSize + BLOCK_SIZE < fileSize) {
        bm->read_block(indirectBlock[blockNum], res + nowSize);
        nowSize += BLOCK_SIZE;
      }

      /* Last one */
      else {
        int len = fileSize - nowSize;
        bm->read_block(indirectBlock[blockNum], buf);
        memcpy(res + nowSize, buf, len);
        nowSize += len;
      }
    }
  }

  /* Change metadata */
  unsigned int time = std::time(0);
  inode->atime = time;
  inode->ctime = time;

  /* return result */
  *size = (int)fileSize;
  *buf_out = res;
  put_inode(inum, inode);
  free(inode);
  return;
}

/* alloc/free blocks if needed */
void
inode_manager::write_file(uint32_t inum, const char *buf, int size)
{
  /*
   * your lab1 code goes here.
   * note: write buf to blocks of inode inum.
   * you need to consider the situation when the size of buf 
   * is larger or smaller than the size of original inode.
   * you should free some blocks if necessary.
   */

  /* Get inode by inum */
  inode_t *inode = get_inode(inum);

  if (!inode || size == 0) {
    return;
  }

  /* Get block num */
  unsigned int oldBlockNum = (inode->size + BLOCK_SIZE - 1) / BLOCK_SIZE;
  unsigned int newBlockNum = (size + BLOCK_SIZE - 1) / BLOCK_SIZE;
  /* BlockNum cannot be more than MAXFILE */
  newBlockNum = MIN(MAXFILE, newBlockNum);

  /* Judge whether blocks should be free and if so, do that */
  if (newBlockNum < oldBlockNum) {
    unsigned int blockNum;
    /* free NDIRECT blocks */
    if (newBlockNum < NDIRECT) {
      for (blockNum = newBlockNum; blockNum < MIN(oldBlockNum, NDIRECT);
        blockNum++) {
        bm->free_block(inode->blocks[blockNum]);
      }
    }

    /* free NINDIRECT blocks */
    if (oldBlockNum > NDIRECT) {
      blockid_t indirectBlock[NINDIRECT];
      bm->read_block(inode->blocks[NDIRECT], (char *)indirectBlock);
      

      if (newBlockNum > NDIRECT) {
        for (blockNum = newBlockNum - NDIRECT; blockNum < oldBlockNum - NDIRECT;
          blockNum++) {
          bm->free_block(indirectBlock[blockNum]);
        }
      } else {
        for (blockNum = 0; blockNum < oldBlockNum - NDIRECT;
          blockNum++) {
          bm->free_block(indirectBlock[blockNum]);
        }

        /* free NDIRECT pointer */
        bm->free_block(inode->blocks[NDIRECT]);
      }
    }
  }

  /* Judge whether blocks should be allocated and if so, do that */
  if (newBlockNum > oldBlockNum) {
    unsigned int blockNum;
    /* Alloc NDIRECT blocks */
    if (oldBlockNum < NDIRECT) {
      for (blockNum = oldBlockNum; blockNum < MIN(newBlockNum, NDIRECT);
        blockNum++) {
        inode->blocks[blockNum] = bm->alloc_block();
      }
    }

    /* Alloc NINDIRECT pointer & NINDIRECT blocks */
    if (newBlockNum > NDIRECT) {
      if (oldBlockNum > NDIRECT) {
        blockid_t indirectBlock[NINDIRECT];
        
        for (blockNum = oldBlockNum - NDIRECT; blockNum < newBlockNum - NDIRECT;
          blockNum++) {
          indirectBlock[blockNum] = bm->alloc_block();
          bm->write_block(inode->blocks[NDIRECT], (char *)indirectBlock);
        }
      } else {
        /* Alloc NINDIRECT pointer */
        inode->blocks[NDIRECT] = bm->alloc_block();
        blockid_t indirectBlock[NINDIRECT];
        
        for (blockNum = 0; blockNum < newBlockNum - NDIRECT;
          blockNum++) {
          indirectBlock[blockNum] = bm->alloc_block();
          bm->write_block(inode->blocks[NDIRECT], (char *)indirectBlock);
        }
      }
    }
  }

  /* Write tha data */
  int nowSize = 0;
  unsigned int blockNum;
  char block[BLOCK_SIZE];
  bzero(block, BLOCK_SIZE);
  /* Write NDIRECT block*/
  for (blockNum = 0; blockNum < NDIRECT && nowSize < size;
   blockNum++) {
    /* Not last one */
    if (nowSize + BLOCK_SIZE < size) {
      bm->write_block(inode->blocks[blockNum], buf + nowSize);
      nowSize += BLOCK_SIZE;
    }

    /* Last one */
    else {
      int len = size - nowSize;
      memcpy(block, buf + nowSize, len);
      bm->write_block(inode->blocks[blockNum], block);
      nowSize += len;
    }
  }

  /* Write NINDIRECT block */
  if (newBlockNum > NDIRECT) {
    blockid_t indirectBlock[NINDIRECT];
    bm->read_block(inode->blocks[NDIRECT], (char *)indirectBlock);
    
    for (blockNum = 0; blockNum < NINDIRECT && nowSize < size;
      blockNum++) {
      /* Not last one */
      if (nowSize + BLOCK_SIZE < size) {
        bm->write_block(indirectBlock[blockNum], buf + nowSize);
        nowSize += BLOCK_SIZE;
      }

      /* Last one */
      else {
        int len = size - nowSize;
        memcpy(block, buf + nowSize, len);
        bm->write_block(indirectBlock[blockNum], block);
        nowSize += len;
      }
    }
  }

  /* Change metadata */
  unsigned int time = std::time(0);
  inode->size = size;
  inode->mtime = time;
  inode->ctime = time;

  put_inode(inum, inode);
  free(inode);
  return;
}

void
inode_manager::getattr(uint32_t inum, extent_protocol::attr &a)
{
  /*
   * your lab1 code goes here.
   * note: get the attributes of inode inum.
   * you can refer to "struct attr" in extent_protocol.h
   */

  inode_t *inode = get_inode(inum);
  if (inode) {
    a.type = inode->type;
    a.atime = inode->atime;
    a.mtime = inode->mtime;
    a.ctime = inode->ctime;
    a.size = inode->size;
    free(inode);
  }

  return;
}

void
inode_manager::remove_file(uint32_t inum)
{
  /*
   * your lab1 code goes here
   * note: you need to consider about both the data block and inode of the file
   * do not forget to free memory if necessary.
   */

  /* Get inode by inum */
  inode_t *inode = get_inode(inum);

  if (!inode) {
    return;
  }

  unsigned int blockNum = (inode->size + BLOCK_SIZE - 1) / BLOCK_SIZE;
  
  /* Free NINDIRECT block */
  if (blockNum > NDIRECT) { 
    blockid_t indirectBlock[NINDIRECT];
    bm->read_block(inode->blocks[NDIRECT], (char *)indirectBlock);

    unsigned int i;
    for (i = 0; i < blockNum - NDIRECT; i++) {
      /* Free NINDIRECT data block */
      bm->free_block(indirectBlock[i]);
    }
    /* Free NINDIRECT pointer */
    bm->free_block(inode->blocks[NDIRECT]);
  }

  /* Free NDIRECT block */
  unsigned int i;
  for (i = 0; i < MIN(blockNum, NDIRECT); i++) {
    bm->free_block(inode->blocks[i]);
  }

  /* Free inode */
  free_inode(inum);

  free(inode);
  return;
}
