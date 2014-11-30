// TODO arrange in alphabetical order, separated by type
#include <cstdlib>
#include <cstdio>
#include <iostream>
#include <atomic>
#include <string>
#include <string.h>
#include <signal.h>
#include <stdint.h>
#include <unistd.h>
#include <fcntl.h>
#include <linux/falloc.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <libaio.h>
#include <pthread.h>
#include <semaphore.h>
#include <ftw.h>
#include <errno.h>

#define BUF_MAX 128
#define FD_MAX 1000
#define Q_MAX 1024

static size_t buffer_size;
static uint64_t  page_size;
static uint64_t num_pages;
std::atomic<unsigned long> read_requests;
std::atomic<unsigned long> write_requests;
static std::string src;
static std::string dst;
io_context_t write_context;
io_context_t read_context;
pthread_t read_worker, write_worker;
static sem_t read_blocking_waiter;
static sem_t write_blocking_waiter;

typedef struct data_obj
{
  struct iocb* m_aiocb;
  size_t m_offset;
  size_t m_file_size;
  int m_src_fd;
  int m_dst_fd;
} data_obj;


void * read_queue(void *) 
{
  //std::cout << "in read q" << std::endl;
  int rc;
  int first_time = 1;
  sem_wait(&read_blocking_waiter);
  while (read_requests > 0){
    io_event event;
    if ((rc = io_getevents(read_context, 1, 1, &event, NULL)) < 1) {
      perror("read getevent error");
      exit(-1);
    }
    data_obj* data = (data_obj*)event.data;
    iocb * cb = (struct iocb*)event.obj;
    // start a corresponding write request
    // init aiocb struct
    struct iocb* w_iocb = (struct iocb*)malloc(sizeof(struct iocb));
    data_obj* w_data = (data_obj *) malloc(sizeof(data_obj));
    bzero ((char *)w_data, sizeof(data_obj));
    bzero ((char *)w_iocb, sizeof(struct iocb));
    // context to be passed to handler
    w_data->m_aiocb = w_iocb;
    w_data->m_offset = data->m_offset;
    w_data->m_file_size = data->m_file_size;
    w_data->m_src_fd = data->m_src_fd;
    w_data->m_dst_fd = data->m_dst_fd;      
    io_prep_pwrite(w_iocb, data->m_dst_fd, cb->u.c.buf, cb->u.c.nbytes, data->m_offset);
    w_iocb->data = w_data;
    //std::cout << "buffer " << std::string((char*)cb->u.c.buf) << std::endl;
    if ((io_submit(write_context, 1, &w_iocb)) < 1) {
      perror("write io submit error");
      exit(-1);
    }
    ++write_requests;
    if (first_time){
      sem_post(&write_blocking_waiter);
      first_time = 0;
    }
    --read_requests;
  }
  return NULL;
}

void * write_queue(void *)
{
  //std::cout << "in write q" << std::endl;
  int rc; 
  sem_wait(&write_blocking_waiter);
  while(write_requests > 0){
    io_event event;
    if ((rc = io_getevents(write_context, 1, 1, &event, NULL)) < 1) {
      perror("write  getevent error");
      exit(-1);
    }
   --write_requests;
   }
   return NULL;
}

int copy_regular (const char* src_file, const char* dst_file)
{
  int src_fd;
  int dst_fd;
  uint64_t num_pages;
  void * buffer_block;
  // get the page_size for the system
  page_size = getpagesize();
  struct stat stat_buf, stat_dst;
  // stat the source file
  if (stat(src_file, &stat_buf) < 0) {
    perror("source file stat error");
    exit(-1);
  }
  // if its a directory, create and exit
  if (S_ISDIR(stat_buf.st_mode)) {
    if (mkdir(dst_file, S_IRWXU | S_IRWXG)) {
      perror("mkdir error");
      exit(-1);
    }
    return 0;
  }
  // open the source file for reading
  if ((src_fd = open(src_file, O_RDONLY | O_DIRECT)) < 0) {
    perror("source file open error");
    exit(-1);
  }
  // open the destination file for writing
  if ((dst_fd = open(dst_file, O_WRONLY| O_CREAT | O_DIRECT, stat_buf.st_mode)) < 0) {
    //std::cout << "file " <<dst_file<<std::endl;
    perror("destination file open error");
    exit(-1);
  }
  if (fstat(dst_fd, &stat_dst)) {
    perror("fstat destination error");
    exit(-1);
  }
  // check if input and output are the same
  if (stat_buf.st_dev  == stat_dst.st_dev && stat_buf.st_ino == stat_dst.st_ino) {
    return 0;
  }
  
  // TODO tell the kernel that we will need the input file
  // posix_fadvise(src_fd, 0, stat_buf.st_size, POSIX_FADV_WILLNEED);
  // more efficient space allocation via fallocate for dst file
  if (fallocate(dst_fd, FALLOC_FL_KEEP_SIZE, 0, stat_buf.st_size) < 0) {
    perror("destination file fallocate");
  }
  // decide the number of pages in the input file and malloc a buffer accordingly
  //num_pages = stat_buf.st_size / page_size + 1;
  buffer_size = page_size; //(num_pages < BUF_MAX) ? (num_pages * page_size) : (BUF_MAX * page_size);
  // now start sending aio read requests
  int first_time = 1;
  for (size_t i = 0; i < stat_buf.st_size; i += buffer_size) {
     int ret  = posix_memalign((void **) &buffer_block, page_size, page_size);
     if (ret != 0) {
       perror("memalign for buffer error");
       exit(-1);
     }
    // init aiocb struct
    struct iocb* r_iocb = (struct iocb*)malloc(sizeof(struct iocb));
    data_obj* r_data = (data_obj *) malloc(sizeof(data_obj));
    bzero ((char *)r_data, sizeof(data_obj));
    bzero ((char *)r_iocb, sizeof(struct iocb));
    // context to be passed to handler
    r_data->m_aiocb = r_iocb;
    r_data->m_offset = i;
    r_data->m_file_size = stat_buf.st_size;
    r_data->m_src_fd = src_fd;
    r_data->m_dst_fd = dst_fd;   
    io_prep_pread(r_iocb, src_fd, buffer_block, buffer_size, i);
    r_iocb->data = r_data;
    if ((io_submit(read_context, 1, &r_iocb)) < 1) {
      perror("read io submit error");
      exit(-1);
    }
    ++read_requests;
    if (first_time) {
      sem_post(&read_blocking_waiter);
      first_time = 0;
    }
  } 
  return 0;
}

std::string split_filename(std::string fname, int depth) 
{
  uint64_t pos;
  pos = fname.length();
  if (fname.find_last_of("/\\", pos -1) == fname.length() - 1) {
    --pos;
  }
  for (uint64_t i = 0; i < depth; ++i){
    pos = fname.find_last_of("/\\", pos -1);
  }
  // std::cout << "split " << fname.substr(pos) << std::endl;
  return fname.substr(pos);
}

int tree_walk (const char* fpath, 
	       const struct stat* sb, 
	       int typeflag,
	       struct FTW* ftwbuf)
{
  if (ftwbuf->level == 0) {
    return 0;
  }
  std::string new_dst_path = dst + split_filename(std::string(fpath), ftwbuf->level);
  copy_regular(fpath, new_dst_path.c_str());
  return 0;
}

std::string format_path(std::string path) 
{
  uint64_t pos;
  pos = path.find('/');
  if (pos == path.length() -1 || pos == std::string::npos ) {
    std::string fpath = "./";
    fpath.append(path);
    return fpath;
  }
  return path;  
}

int main(int argc, char * argv[])
{
  read_requests = write_requests = 0;
  sem_init(&read_blocking_waiter, 0, 0);
  sem_init(&write_blocking_waiter, 0, 0);
  src = argv[1];
  dst = argv[2];
  uint64_t i, rc;
  src = format_path(src);
  dst = format_path(dst);
  // set up read and write notification threads
  if ((rc = pthread_create(&read_worker, NULL, read_queue, NULL))) {
    perror("read thread creation error");
    exit(-1);
  }
  if ((rc = pthread_create(&write_worker, NULL, write_queue, NULL))) {
    perror("write thread creation error");
    exit(-1);
  }
  bzero((char *)&read_context, sizeof(read_context));
  bzero((char *)&write_context, sizeof(write_context));
  if ((rc = io_queue_init(Q_MAX, &read_context))) {
    perror("read context queue init error");
    exit(-1);
  }
  if ((rc = io_queue_init(Q_MAX, &write_context))) {
    perror("write context queue init error");
    exit(-1);
  }
  struct stat src_stat, dst_stat;
  if (stat(src.c_str(), &src_stat)) {
    perror("source file stat error");
    exit(-1);
  }
  if (stat(dst.c_str(), &dst_stat)) {
    // if error, must be because of a no entry
    if (errno != ENOENT) {
      perror("destination file stat error");
      exit(-1);
    }
    // new now check if we need to copy a file or directory
    if (S_ISDIR(src_stat.st_mode)) {
      // try creating the root at the destination
      if(mkdir(dst.c_str(), S_IRWXU | S_IRWXG)) {
	perror("destination mkdir failed");
	exit(-1);
      }
      // traverse the entire tree and copy files or directories 
      if (nftw(src.c_str(), tree_walk, FD_MAX, FTW_PHYS)) {
	perror("nftw traversal error");
	exit(-1);
      }     
    }
    else { // is a file
      copy_regular(src.c_str(), dst.c_str());
    }
  }
  else { // dst already exists
    if (S_ISDIR(src_stat.st_mode)) {
      // if dir -> file error
      if (!S_ISDIR(dst_stat.st_mode)) {
	perror ("cannot copy directory to non-directory");
	exit(-1);
      }
      // dir -> dir
      dst.append(split_filename(src, 1));
      //std::cout <<"dst " << dst << "\n";
      if (mkdir(dst.c_str(),  S_IRWXU | S_IRWXG)) {
	perror("destination mkdir failed");
	exit(-1);
      }
      if (nftw(src.c_str(), tree_walk, FD_MAX, FTW_PHYS)) {
	perror("nftw traversal error");
	exit(-1);
      }
    }
    else {
      if (!S_ISDIR(dst_stat.st_mode)) {
	// file -> file overwrite
	copy_regular(src.c_str(), dst.c_str());
      }
      else {
	// file -> dir
	dst.append(split_filename(src, 1));
	copy_regular(src.c_str(), dst.c_str());
      }
    }
  }
  pthread_join(read_worker, NULL);
  pthread_join(write_worker, NULL);
  return 0;
}


