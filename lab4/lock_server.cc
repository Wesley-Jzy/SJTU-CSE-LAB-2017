// the lock server implementation

#include "lock_server.h"
#include <sstream>
#include <stdio.h>
#include <unistd.h>
#include <arpa/inet.h>

lock_server::lock_server():
  nacquire (0)
{
  lock_granted.clear();
  lock_stat.clear();
  pthread_mutex_init(&mutex, NULL);
  pthread_cond_init(&cond, NULL);
}

lock_protocol::status
lock_server::stat(int clt, lock_protocol::lockid_t lid, int &r)
{
  lock_protocol::status ret = lock_protocol::OK;
  printf("stat request from clt %d\n", clt);

  pthread_mutex_lock(&mutex);
  r = lock_stat[lid];
  pthread_mutex_unlock(&mutex);

  return ret;
}

lock_protocol::status
lock_server::acquire(int clt, lock_protocol::lockid_t lid, int &r)
{
  lock_protocol::status ret = lock_protocol::OK;
	// Your lab4 code goes here
  pthread_mutex_lock(&mutex);
  if (lock_granted.find(lid) != lock_granted.end()) {
    while (lock_granted[lid] == true) {
      pthread_cond_wait(&cond, &mutex);
    }
  }

  lock_granted[lid] = true;
  lock_stat[lid]++;

  pthread_mutex_unlock(&mutex);
  return ret;
}

lock_protocol::status
lock_server::release(int clt, lock_protocol::lockid_t lid, int &r)
{
  lock_protocol::status ret = lock_protocol::OK;
	// Your lab4 code goes here
  pthread_mutex_lock(&mutex);

  if (lock_granted.find(lid) == lock_granted.end() || lock_granted[lid] == false) {
    ret = lock_protocol::NOENT;
    return ret;
  }

  lock_granted[lid] = false;

  pthread_cond_broadcast(&cond);
  pthread_mutex_unlock(&mutex);

  return ret;
}
