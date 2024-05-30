package com.hmdp.utils;

public interface ILock {
//    尝试获取锁
      boolean tryLock(long timeoutSec);//锁持有的超过时间，达到期限释放
//    释放锁
      void unLock();
}
