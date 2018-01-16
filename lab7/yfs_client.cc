// yfs client.  implements FS operations using extent and lock server
#include "yfs_client.h"
#include "extent_client.h"
#include <sstream>
#include <iostream>
#include <stdio.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>

yfs_client::yfs_client()
{
  ec = NULL;
  lc = NULL;
}

yfs_client::yfs_client(std::string extent_dst, std::string lock_dst, const char* cert_file)
{
  ec = new extent_client(extent_dst);
  lc = new lock_client(lock_dst);
  if (ec->put(1, "") != extent_protocol::OK)
      printf("error init root dir\n"); // XYB: init root dir
}


int
yfs_client::verify(const char* name, unsigned short *uid)
{
  	int ret = OK;

	return ret;
}

yfs_client::inum
yfs_client::n2i(std::string n)
{
    std::istringstream ist(n);
    unsigned long long finum;
    ist >> finum;
    return finum;
}

std::string
yfs_client::filename(inum inum)
{
    std::ostringstream ost;
    ost << inum;
    return ost.str();
}

bool 
yfs_client::isfile(inum inum)
{
    lc->acquire(inum);
    bool ret = isfile_withoutLock(inum);
    lc->release(inum);
    return ret;
}

bool
yfs_client::isfile_withoutLock(inum inum)
{
    extent_protocol::attr a;

    if (ec->getattr(inum, a) != extent_protocol::OK) {
        printf("error getting attr\n");
        return false;
    }

    if (a.type == extent_protocol::T_FILE) {
        printf("isfile: %lld is a file\n", inum);
        return true;
    } 

    printf("isfile: %lld is not a file\n", inum);
    return false;
}
/** Your code here for Lab...
 * You may need to add routines such as
 * readlink, issymlink here to implement symbolic link.
 * 
 * */

bool
yfs_client::isdir(inum inum)
{
    lc->acquire(inum);
    bool ret = isdir_withoutLock(inum);
    lc->release(inum);
    return ret;
}

bool
yfs_client::isdir_withoutLock(inum inum)
{
    // Oops! is this still correct when you implement symlink?
    extent_protocol::attr a;

    if (ec->getattr(inum, a) != extent_protocol::OK) {
        printf("error getting attr\n");
        return false;
    }

    if (a.type == extent_protocol::T_DIR) {
        printf("isdir: %lld is a dir\n", inum);
        return true;
    } 
    printf("isdir: %lld is not a dir\n", inum);
    return false;
}

int
yfs_client::getfile(inum inum, fileinfo &fin)
{
    lc->acquire(inum);
    int ret = getfile_withoutLock(inum, fin);
    lc->release(inum);
    return ret;
}

int
yfs_client::getfile_withoutLock(inum inum, fileinfo &fin)
{
    int r = OK;

    printf("getfile %016llx\n", inum);
    extent_protocol::attr a;
    if (ec->getattr(inum, a) != extent_protocol::OK) {
        r = IOERR;
        goto release;
    }

    fin.atime = a.atime;
    fin.mtime = a.mtime;
    fin.ctime = a.ctime;
    fin.size = a.size;
    printf("getfile %016llx -> sz %llu\n", inum, fin.size);

release:
    return r;
}

int
yfs_client::getdir(inum inum, dirinfo &din)
{
    lc->acquire(inum);
    int ret = getdir_withoutLock(inum, din);
    lc->release(inum);
    return ret;
}

int
yfs_client::getdir_withoutLock(inum inum, dirinfo &din)
{
    int r = OK;

    printf("getdir %016llx\n", inum);
    extent_protocol::attr a;
    if (ec->getattr(inum, a) != extent_protocol::OK) {
        r = IOERR;
        goto release;
    }
    din.atime = a.atime;
    din.mtime = a.mtime;
    din.ctime = a.ctime;

release:
    return r;
}


#define EXT_RPC(xx) do { \
    if ((xx) != extent_protocol::OK) { \
        printf("EXT_RPC Error: %s:%d \n", __FILE__, __LINE__); \
        r = IOERR; \
        goto release; \
    } \
} while (0)


int
yfs_client::setattr(inum ino, filestat st, unsigned long toset)
{
    lc->acquire(ino);
    int ret = setattr_withoutLock(ino, st, toset);
    lc->release(ino);
    return ret;
}

// Only support set size of attr
int
yfs_client::setattr_withoutLock(inum ino, filestat st, unsigned long toset)
{
    int r = OK;

    /*
     * your lab2 code goes here.
     * note: get the content of inode ino, and modify its content
     * according to the size (<, =, or >) content length.
     */

    /* Get ino */
    std::string buf;
    r = ec->get(ino, buf);
    if (r != OK) {
        printf("setattr error\n");
        return r;
    }

    /* Modify the size and store */
    buf.resize(st.size);

    r = ec->put(ino, buf);
    if (r != OK) {
        printf("setattr error\n");
        return r;
    }

    return r;
}

int
yfs_client::create(inum parent, const char *name, mode_t mode, inum &ino_out)
{
    lc->acquire(parent);
    int ret = create_withoutLock(parent, name, mode, ino_out);
    lc->release(parent);
    return ret;
}

int
yfs_client::create_withoutLock(inum parent, const char *name, mode_t mode, inum &ino_out)
{
    int r = OK;

    /*
     * your lab2 code goes here.
     * note: lookup is what you need to check if file exist;
     * after create file or dir, you must remember to modify the parent infomation.
     */

    /* Lookup whether name exists */
    bool found = false;
    r = lookup_withoutLock(parent, name, found, ino_out);
    if (found) {
        printf("file has existed\n");
        return r;
    }

    /* Create the file */
    ec->create(extent_protocol::T_FILE, ino_out);

    /* Modify the parent */
    std::string buf;
    std::string entry = std::string(name) + '/' +  filename(ino_out) + '/';
    r = ec->get(parent, buf);
    if (r != OK) {
        printf("create error\n");
        return r;
    }

    buf.append(entry);

    r = ec->put(parent, buf);
    if (r != OK) {
        printf("create error\n");
        return r;
    }

    return r;
}

int
yfs_client::mkdir(inum parent, const char *name, mode_t mode, inum &ino_out)
{
    lc->acquire(parent);
    int ret = mkdir_withoutLock(parent, name, mode, ino_out);
    lc->release(parent);
    return ret;
}

int
yfs_client::mkdir_withoutLock(inum parent, const char *name, mode_t mode, inum &ino_out)
{
    int r = OK;

    /*
     * your lab2 code goes here.
     * note: lookup is what you need to check if directory exist;
     * after create file or dir, you must remember to modify the parent infomation.
     */

    /* Lookup whether name exists */
    bool found = false;
    r = lookup_withoutLock(parent, name, found, ino_out);
    if (found) {
        printf("directory has existed\n");
        return r;
    }

    /* Create the directory */
    ec->create(extent_protocol::T_DIR, ino_out);

    /* Modify the parent */
    std::string buf;
    std::string entry = std::string(name) + '/' +  filename(ino_out) + '/';
    r = ec->get(parent, buf);
    if (r != OK) {
        printf("create error\n");
        return r;
    }

    buf.append(entry);

    r = ec->put(parent, buf);
    if (r != OK) {
        printf("create error\n");
        return r;
    }

    return r;
}

int
yfs_client::lookup(inum parent, const char *name, bool &found, inum &ino_out)
{
    lc->acquire(parent);
    int ret = lookup_withoutLock(parent, name, found, ino_out);
    lc->release(parent);
    return ret;
}

int
yfs_client::lookup_withoutLock(inum parent, const char *name, bool &found, inum &ino_out)
{
    int r = OK;

    /*
     * your lab2 code goes here.
     * note: lookup file from parent dir according to name;
     * you should design the format of directory content.
     */

    found = false;
    /* Judge whether parent exists */
    if (!isdir_withoutLock(parent)) {
        return r;
    }

    /* Read parent dir */
    std::list<yfs_client::dirent> entries;
    r = readdir_withoutLock(parent, entries);
    if (r != OK) {
        printf("Wrong in lookup\n");
        return r;
    }

    /* Lookup file by name */
    std::list<yfs_client::dirent>::iterator it = entries.begin();
    while (it != entries.end()) {
        std::string filename = it->name;
        if (filename == std::string(name)) {
            found = true;
            ino_out = it->inum;
            r = EXIST;
            return r;
        }
        it++;
    }

    return r;
}

int
yfs_client::readdir(inum dir, std::list<dirent> &list)
{
    lc->acquire(dir);
    int ret = readdir_withoutLock(dir, list);
    lc->release(dir);
    return ret;
}

int
yfs_client::readdir_withoutLock(inum dir, std::list<dirent> &list)
{
    int r = OK;

    /*
     * your lab2 code goes here.
     * note: you should parse the dirctory content using your defined format,
     * and push the dirents to the list.
     */

    /* 
     * Use '/' as the seperator, 
     * A typical dirent is filename/inum/
     * Thus, '/' is not allowed in filename 
     */ 

    /* Read buf from block */
    std::string buf;
    r = ec->get(dir, buf);
    if (r != OK) {
        printf("Wrong in readdir\n");
        return r;
    }

    /* Get directory entries */
    unsigned int head = 0;
    unsigned int tail = 0;
    struct dirent *entry = new dirent(); 
    while (head < buf.size()) {
        /* Get name */
        tail = buf.find('/', head);
        std::string name = buf.substr(head, tail - head);
        entry->name = name;
        head = tail + 1;

        /* Get inum */
        tail = buf.find('/', head);
        std::string inum = buf.substr(head, tail - head);
        entry->inum = n2i(inum);
        head = tail + 1;

        list.push_back(*entry);
    }
    delete entry;

    return r;
}

int
yfs_client::read(inum ino, size_t size, off_t off, std::string &data)
{
    lc->acquire(ino);
    int ret = read_withoutLock(ino, size, off, data);
    lc->release(ino);
    return ret;
}

int
yfs_client::read_withoutLock(inum ino, size_t size, off_t off, std::string &data)
{
    int r = OK;

    /*
     * your lab2 code goes here.
     * note: read using ec->get().
     */

    std::string buf;
    r = ec->get(ino, buf);
    if (r != OK) {
        printf("Wrong in read\n");
        return r;
    }

    if ((int)buf.size() <= off) {
        data = "";
    } else {
        data = buf.substr(off, size);
    }

    return r;
}

int
yfs_client::write(inum ino, size_t size, off_t off, const char *data,
        size_t &bytes_written)
{
    lc->acquire(ino);
    int ret = write_withoutLock(ino, size, off, data, bytes_written);
    lc->release(ino);
    return ret;
}

int
yfs_client::write_withoutLock(inum ino, size_t size, off_t off, const char *data,
        size_t &bytes_written)
{
    int r = OK;

    /*
     * your lab2 code goes here.
     * note: write using ec->put().
     * when off > length of original file, fill the holes with '\0'.
     */

    std::string buf;
    r = ec->get(ino, buf);
    if (r != OK) {
        printf("Wrong in write\n");
        return r;
    }

    /* Handle off > bufsize and off+size > bufsize */
    if (off + size  > buf.size()) {
        buf.resize(off + size);
    }
    
    buf.replace(off, size, std::string(data, size));

    r = ec->put(ino, buf);
    if (r != OK) {
        printf("Wrong in write\n");
        return r;
    }
    bytes_written = size;

    return r;
}

int 
yfs_client::unlink(inum parent,const char *name)
{
    lc->acquire(parent);
    int ret = unlink_withoutLock(parent, name);
    lc->release(parent);
    return ret;
}

int 
yfs_client::unlink_withoutLock(inum parent,const char *name)
{
    int r = OK;

    /*
     * your lab2 code goes here.
     * note: you should remove the file using ec->remove,
     * and update the parent directory content.
     */

    /* Lookup whether name exists */
    inum ino_out;
    bool found = false;
    r = lookup_withoutLock(parent, name, found, ino_out);
    if (!found) {
        printf("file doesn't exist\n");
        r = NOENT;
        return r;
    }


    /* Remove the file */
    ec->remove(ino_out);

    /* Modify the parent */
    std::string buf;
    std::string entry = std::string(name) + '/' +  filename(ino_out) + '/';
    r = ec->get(parent, buf);
    if (r != OK) {
        printf("create error\n");
        return r;
    }

    int pos = buf.find(entry);
    buf.replace(pos, entry.size(), "");

    r = ec->put(parent, buf);
    if (r != OK) {
        printf("create error\n");
        return r;
    }

    return r;
}

int 
yfs_client::symlink(inum parent, const char* name, const char* link, inum& ino_out)
{
    lc->acquire(parent);
    int ret = symlink_withoutLock(parent, name, link, ino_out);
    lc->release(parent);
    return ret;
}

int 
yfs_client::symlink_withoutLock(inum parent, const char* name, const char* link, inum& ino_out)
{
    int r = OK;

    /* Lookup */
    bool found = false;
    r = lookup_withoutLock(parent, name, found, ino_out);
    if (found) {
        printf("symlink error\n");
        r = EXIST;
        return r;
    }

    /* Create a file */
    ec->create(extent_protocol::T_SYMLINK, ino_out);
    /* Write link into file */
    ec->put(ino_out, std::string(link));

    /* Modify the parent */
    std::string buf;
    std::string entry = std::string(name) + '/' +  filename(ino_out) + '/';
    r = ec->get(parent, buf);
    if (r != OK) {
        printf("symlink error\n");
        return r;
    }

    buf.append(entry);

    r = ec->put(parent, buf);
    if (r != OK) {
        printf("symlink error\n");
        return r;
    }
    
    return r;
}

int 
yfs_client::readlink(inum ino, std::string &link)
{
    lc->acquire(ino);
    int ret = readlink_withoutLock(ino, link);
    lc->release(ino);
    return ret;
}

int 
yfs_client::readlink_withoutLock(inum ino, std::string &link)
{
    int r = OK;

    std::string buf;
    r = ec->get(ino, buf);
    if(r != OK) {
        printf("readlink error\n");
        return r;
    }

    link = buf;

    return r;
}

void  
yfs_client::commit()
{
    ec->commit();
}

void 
yfs_client::undo() 
{
    ec->undo();
}


void 
yfs_client::redo()
{
    ec->redo();
}
