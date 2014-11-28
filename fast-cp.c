// TODO arrange in alphabetical order, separated by type
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <signal.h>
#include <stdint.h>
#include <unistd.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <aio.h>
#include <semaphore.h>
#include <ftw.h>
#include <errno.h>

#define BUF_MAX 128

size_t buffer_size;
uint64_t  page_size;
uint64_t num_pages;
uint64_t num_requests;
sem_t blocking_waiter;

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
  handler_context* hctx = (handler_context*)sigval.sival_ptr;
  if (aio_error(hctx->m_aiocb)) {
    perror("read aio error");
    exit(-1);
  }
  nbytes = aio_return(hctx->m_aiocb);
  int i = 0;
  void * buffer = (void *)hctx->m_aiocb->aio_buf;
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

  sem_post(&blocking_waiter);

  if (aio_write(w_aiocb) < 0) {
    perror("aio_write error");
    exit(-1);
  }
  ++num_requests;
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

int copy_regular (const char* src, const char* dst)
{
  int src_fd;
  int dst_fd;
  uint64_t num_pages;
  void * buffer_block;
  // get the page_size for the system
  page_size = getpagesize();
  struct stat stat_buf;
  // open the source file for reading
  if ((src_fd = open(src, O_RDONLY)) < 0) {
    perror("source file open error");
    exit(-1);
  }
  // stat the input file
  if (fstat(src_fd, &stat_buf) < 0) {
    perror("source file stat error");
    exit(-1);
  }
  // open the destination file for writing
  if ((dst_fd = open(dst, O_WRONLY| O_CREAT, stat_buf.st_mode)) < 0) {
    perror("destination file open error");
    exit(-1);
  }
  // TODO tell the kernel that we will need the input file
  
  // more efficient space allocation via fallocate for dst file
  if (fallocate(dst_fd, 0, 0, stat_buf.st_size) < 0) {
    perror("destination file fallocate");
  }
  // decide the number of pages in the input file and malloc a buffer accordingly
  num_pages = stat_buf.st_size / page_size + 1;
  buffer_size = page_size; //(num_pages < BUF_MAX) ? (num_pages * page_size) : (BUF_MAX * page_size);
  buffer_block = (void *)malloc(buffer_size);
  if (errno == ENOMEM) {
    perror("malloc for buffer error");
    exit(-1);
  }
  // now start sending aio read requests
  size_t i;
  for (i = 0; i < stat_buf.st_size; i += buffer_size) {
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


int copy_directory(
		   const char* fpath, 
		   const struct stat* sb, 
		   int typeflag,
		   struct FTW* ftwbuf)
{


}

int main(int argc, char * argv[])
{
  num_requests = 0;
  sem_init(&blocking_waiter, 0, 0);
  copy_regular(argv[1], argv[2]);
  uint64_t i;
  for (i = 0; i < num_requests; ++i) {
    sem_wait(&blocking_waiter);
  }
  sem_destroy(&blocking_waiter);
  return 0;
}


