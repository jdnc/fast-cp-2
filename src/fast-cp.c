// TODO arrange in alphabetical order, separated by type
#include <cstdlib>
#include <cstdio>
#include <atomic>
#include <iostream>
#include <string>
#include <string.h>
#include <signal.h>
#include <stdint.h>
#include <unistd.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/time.h>
#include <aio.h>
#include <semaphore.h>
#include <ftw.h>
#include <errno.h>
#include <time.h>


#define BUF_MAX 128
#define FD_MAX 1000

static size_t buffer_size;
static uint64_t  page_size;
static uint64_t num_pages;
std::atomic<unsigned long> num_requests;
static sem_t blocking_waiter;
static std::string src;
static std::string dst;

void aio_write_handler(sigval_t signal);

typedef struct handler_context
{
  struct aiocb* m_aiocb;
  size_t m_offset;
  size_t m_file_size;
  int m_src_fd;
  int m_dst_fd;
} handler_context;


void aio_read_handler (sigval_t  sigval)
{
  size_t nbytes;
  size_t w_nbytes = 0;
  handler_context* hctx = (handler_context*)sigval.sival_ptr;
  if (aio_error(hctx->m_aiocb)) {
    perror("read aio error");
    exit(-1);
  }
  nbytes = aio_return(hctx->m_aiocb);
  int i = 0;
  void * buffer = (void *)hctx->m_aiocb->aio_buf;
  /*w_nbytes = pwrite(hctx->m_dst_fd, buffer, nbytes, hctx->m_offset);
  if (w_nbytes != nbytes) {
    perror("sync write error");
    exit(-1);
    }
    sem_post(&blocking_waiter);*/
  // now send an async write request for the destination file
  // init aiocb struct
  struct aiocb*  w_aiocb = (struct aiocb*)malloc(sizeof(struct aiocb));
  handler_context* w_context = (handler_context *) malloc(sizeof(handler_context));
  bzero ((char *)w_context, sizeof(handler_context));
  bzero ((char *)w_aiocb, sizeof(struct aiocb));
  // context to be passed to handler
  w_context->m_aiocb = w_aiocb;
  w_context->m_offset = hctx->m_offset;
  w_context->m_file_size = hctx->m_file_size;
  w_context->m_src_fd = hctx->m_src_fd;
  w_context->m_dst_fd = hctx->m_dst_fd;

  // basic setup
  w_aiocb->aio_fildes = hctx->m_dst_fd;
  w_aiocb->aio_nbytes = nbytes;
  w_aiocb->aio_offset = hctx->m_offset;
  w_aiocb->aio_buf = buffer;

  // thread callback
  w_aiocb->aio_sigevent.sigev_notify = SIGEV_THREAD;
  w_aiocb->aio_sigevent.sigev_notify_function = aio_write_handler;
  w_aiocb->aio_sigevent.sigev_notify_attributes = NULL;
  w_aiocb->aio_sigevent.sigev_value.sival_ptr = (void *)w_context;

  if (aio_write(w_aiocb) < 0) {
    perror("aio_write error");
    exit(-1);
  }
  ++num_requests;
  sem_post(&blocking_waiter);
}

void aio_write_handler (sigval_t sigval)
{
  size_t nbytes;
  handler_context* hctx = (handler_context*)sigval.sival_ptr;
  if (aio_error(hctx->m_aiocb)) {
    perror("write aio error");
    exit(-1);
  }
  nbytes = aio_return(hctx->m_aiocb);
  sem_post(&blocking_waiter);
  //free(hctx->m_aiocb->aio_buf);
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
  if ((src_fd = open(src_file, O_RDONLY)) < 0) {
    perror("source file open error");
    exit(-1);
  }
  // open the destination file for writing
  if ((dst_fd = open(dst_file, O_WRONLY| O_CREAT, stat_buf.st_mode)) < 0) {
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
  posix_fadvise(src_fd, 0, stat_buf.st_size, POSIX_FADV_WILLNEED);
  // more efficient space allocation via fallocate for dst file
  if (fallocate(dst_fd, 0, 0, stat_buf.st_size) < 0) {
    perror("destination file fallocate");
  }
  // decide the number of pages in the input file and malloc a buffer accordingly
  num_pages = stat_buf.st_size / page_size + 1;
  buffer_size = page_size; //(num_pages < BUF_MAX) ? (num_pages * page_size) : (BUF_MAX * page_size);
  // now start sending aio read requests
  size_t i;
  for (i = 0; i < stat_buf.st_size; i += buffer_size) {
    //posix_fadvise(src_fd, i, buffer_size, POSIX_FADV_SEQUENTIAL);
    buffer_block = (void *)malloc(buffer_size);
     if (errno == ENOMEM) {
       perror("malloc for buffer error");
       exit(-1);
     }
    // init aiocb struct
    struct aiocb* r_aiocb = (struct aiocb*)malloc(sizeof(struct aiocb));
    handler_context* r_context = (handler_context *) malloc(sizeof(handler_context));
    bzero ((char *)r_context, sizeof(handler_context));
    bzero ((char *)r_aiocb, sizeof(struct aiocb));
    // context to be passed to handler
    r_context->m_aiocb = r_aiocb;
    r_context->m_offset = i;
    r_context->m_file_size = stat_buf.st_size;
    r_context->m_src_fd = src_fd;
    r_context->m_dst_fd = dst_fd;
    
    // basic setup
    r_aiocb->aio_fildes = src_fd;
    r_aiocb->aio_nbytes = buffer_size;
    r_aiocb->aio_offset = i;
    r_aiocb->aio_buf = buffer_block;
    
    // thread callback
    r_aiocb->aio_sigevent.sigev_notify = SIGEV_THREAD;
    r_aiocb->aio_sigevent.sigev_notify_function = aio_read_handler;
    r_aiocb->aio_sigevent.sigev_notify_attributes = NULL;
    r_aiocb->aio_sigevent.sigev_value.sival_ptr = (void *)r_context;

    if (aio_read(r_aiocb) < 0) {
      perror("aio_read error");
      exit(-1);
    }
    ++num_requests;
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
  if (argc != 3) {
    printf("usage : %s <source> <destination>\n.", argv[0]);
    return 0;
  }
  struct timespec tv1, tv2;
  num_requests = 0;
  clock_gettime(CLOCK_MONOTONIC, &tv1);
  sem_init(&blocking_waiter, 0, 0);
  //copy_regular(argv[1], argv[2]);
  src = argv[1];
  dst = argv[2];
  uint64_t i;
  src = format_path(src);
  dst = format_path(dst);
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
  for (i = 0; i < num_requests; ++i) {
    sem_wait(&blocking_waiter);
  }
  sem_destroy(&blocking_waiter);
  clock_gettime(CLOCK_MONOTONIC, &tv2);
  uint64_t tv = (tv2.tv_sec - tv1.tv_sec) * 1000000000+ tv2.tv_nsec -tv1.tv_nsec;
  printf("completion time = %ld.%06ld s\n", tv / 1000000000, tv % 1000000000);
  return 0;
}


