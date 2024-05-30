package com.hmdp.service.impl;

import cn.hutool.core.bean.BeanUtil;
import com.hmdp.dto.Result;
import com.hmdp.entity.SeckillVoucher;
import com.hmdp.entity.Voucher;
import com.hmdp.entity.VoucherOrder;
import com.hmdp.mapper.VoucherOrderMapper;
import com.hmdp.service.ISeckillVoucherService;
import com.hmdp.service.IVoucherOrderService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.utils.RedisIdWorker;
import com.hmdp.utils.SimpleRedisLock;
import com.hmdp.utils.UserHolder;
import org.redisson.api.RLock;
import org.redisson.api.RedissonClient;
import org.springframework.aop.framework.AopContext;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.redis.connection.stream.*;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import javax.annotation.PostConstruct;
import javax.annotation.Resource;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static com.hmdp.utils.RedisConstants.SECKILL_STOCK_KEY;

@Service
public class VoucherOrderServiceImpl extends ServiceImpl<VoucherOrderMapper, VoucherOrder> implements IVoucherOrderService {

    @Resource
    private ISeckillVoucherService seckillVoucherService;
    @Resource
    private RedisIdWorker redisIdWorker;
    @Resource
    private StringRedisTemplate stringRedisTemplate;
    @Resource
    private RedissonClient redissonClient;

    @Override
    public Result seckillVoucher(Long voucherId) {
        // 1.查询优惠券
        SeckillVoucher voucher = seckillVoucherService.getById(voucherId);
        // 2.判断秒杀是否开始
        if (voucher.getBeginTime().isBefore(LocalDateTime.now())) {
            // 尚未开始
            return Result.fail("秒杀尚未开始！");
        }
        // 3.判断秒杀是否已经结束
        if (voucher.getEndTime().isAfter(LocalDateTime.now())) {
            //已经结束
            return Result.fail("秒杀已经结束！");
        }
        // 4.判断库存是否充足
        if (voucher.getStock() < 1) {
            // 库存不足
            return Result.fail("库存不足！");
        }
//        一人一单
        long userId = UserHolder.getUser().getId();
        //乐观锁思想解决超卖问题
//        int count = query().eq("user_id",userId).eq("voucher_id",voucherId).count();
//        if (count > 0){
//            return Result.fail("该用户已经下单过！");
//        }
//        //5，扣减库存
//        boolean success = seckillVoucherService.update().setSql("stock = stock - 1").
//                eq("voucher_id",voucherId).gt("stock",0).update();
//        //扣减库存
//        if (!success){
//            return Result.fail("扣减失败！");
//        }
//        创建锁，分布式锁解决超卖问题
        SimpleRedisLock lock = new SimpleRedisLock("order" + userId, stringRedisTemplate);
//        Redisson分布式锁解决超卖问题
//        RLock lock = redissonClient.getLock("lock:order:" + userId);
//        boolean isLock = lock.tryLock();
        boolean isLock = lock.tryLock(1200);
        if (!isLock) {
            return Result.fail("该用户已经下单过！");
        }
        try {
            //获取代理对象(事务)
            IVoucherOrderService proxy = (IVoucherOrderService) AopContext.currentProxy();
            return proxy.createVoucherOrder(voucherId);
        } finally {
            //释放锁
            lock.unLock();
        }
        //6.创建订单
//        VoucherOrder voucherOrder = new VoucherOrder();
//        //6.1.用户id
//        voucherOrder.setUserId(userId);
//        // 6.2.订单id
//        long orderId = redisIdWorker.nextId("order");
//        voucherOrder.setId(orderId);
//        // 6.3.代金券id
//        voucherOrder.setVoucherId(voucherId);
//          save(voucherOrder);
//        return Result.ok(orderId);
    }

    @Transactional
    public Result createVoucherOrder(Long voucherId) {
        Long userId = UserHolder.getUser().getId();
        synchronized (userId.toString().intern()) {
            // 5.1.查询订单
            int count = query().eq("user_id", userId).eq("voucher_id", voucherId).count();
            // 5.2.判断是否存在
            if (count > 0) {
                // 用户已经购买过了
                return Result.fail("用户已经购买过一次！");
            }
            // 6.扣减库存
            boolean success = seckillVoucherService.update()
                    .setSql("stock = stock - 1") // set stock = stock - 1
                    .eq("voucher_id", voucherId).gt("stock", 0) // where id = ? and stock > 0
                    .update();
            if (!success) {
                // 扣减失败
                return Result.fail("库存不足！");
            }
            // 7.创建订单
            VoucherOrder voucherOrder = new VoucherOrder();
            // 7.1.订单id
            long orderId = redisIdWorker.nextId("order");
            voucherOrder.setId(orderId);
            // 7.2.用户id
            voucherOrder.setUserId(userId);
            // 7.3.代金券id
            voucherOrder.setVoucherId(voucherId);
            save(voucherOrder);
            // 7.返回订单id
            return Result.ok(orderId);
        }
    }

    // 基于阻塞队列实现秒杀优化
    //异步处理线程池
    private static final ExecutorService SECKILL_ORDER_EXECUTOR_A= Executors.newSingleThreadExecutor();

    @PostConstruct
    private void init1(){
        SECKILL_ORDER_EXECUTOR_A.submit(new VoucherOrderHandler1());
    }
    private class VoucherOrderHandler1 implements Runnable{

        @Override
        public void run() {
            while (true){
                try {
                    // 1.获取队列中的订单信息
                    VoucherOrder voucherOrder = orderTasks.take();
                    // 2.创建订单
                    handleVoucherOrder(voucherOrder);
                } catch (Exception e) {
                    log.error("处理订单异常", e);
                }
            }
        }
    }
    private void handleVoucherOrder(VoucherOrder voucherOrder) {
        //1.获取用户
        Long userId = voucherOrder.getUserId();
        // 2.创建锁对象
        RLock redisLock = redissonClient.getLock("lock:order:" + userId);
        // 3.尝试获取锁
        boolean isLock = redisLock.tryLock();
        // 4.判断是否获得锁成功
        if (!isLock) {
            // 获取锁失败，直接返回失败或者重试
            log.error("不允许重复下单！");
            return;
        }
        try {
            //注意：由于是spring的事务是放在threadLocal中，此时的是多线程，事务会失效
            proxy1.createVoucherOrder1(voucherOrder);
        } finally {
            // 释放锁
            redisLock.unlock();
        }
    }
    private static final DefaultRedisScript<Long> SECKILL_SCRIPT;
    static {
        SECKILL_SCRIPT = new DefaultRedisScript<>();
        SECKILL_SCRIPT.setLocation(new ClassPathResource("seckill.lua"));
        SECKILL_SCRIPT.setResultType(Long.class);
    }
    private BlockingQueue<VoucherOrder> orderTasks =new  ArrayBlockingQueue<>(1024 * 1024);
    private IVoucherOrderService proxy1;
    @Override
    public Result seckillVoucher1(Long voucherId) {
//        获取当前用户
        Long userId = UserHolder.getUser().getId();
//        执行lua脚本
        Long result = stringRedisTemplate.execute(SECKILL_SCRIPT,Collections.emptyList(),
                voucherId.toString(),userId.toString());
        int r = result.intValue();
        if (r != 0){
            return Result.fail(r == 1 ? "库存不足" : "不能重复下单");
        }
//        有购买资格，下单信息保存到阻塞队列
        VoucherOrder voucherOrder = new VoucherOrder();
        long orderId = redisIdWorker.nextId("order");
        voucherOrder.setId(orderId);
        voucherOrder.setUserId(userId);
        voucherOrder.setVoucherId(voucherId);
//        放入阻塞队列
        orderTasks.add(voucherOrder);
//        获取代理对象
        proxy1 = (IVoucherOrderService) AopContext.currentProxy();
        return Result.ok(orderId);
    }

    @Transactional
    public void createVoucherOrder1(VoucherOrder voucherOrder){
        Long userId = voucherOrder.getUserId();
//        查询订单
        int count = query().eq("user_id",userId).eq("voucher_id",voucherOrder.getVoucherId()).count();
        if (count > 0){
            log.error("用户已经购买过一次！");
            return;
        }
        boolean success = seckillVoucherService.update().setSql("stock = stock - 1").
                eq("voucher_id",voucherOrder.getVoucherId()).gt("stock",0).update();
        if (!success){
            log.error("扣减库存失败！");
            return;
        }
        save(voucherOrder);
    }



    //    基于Redis的Stream结构作为消息队列，实现异步秒杀下单
    private static final ExecutorService SECKILL_ORDER_EXECUTOR_B = Executors.newSingleThreadExecutor();
    @PostConstruct
    private void init2(){
        SECKILL_ORDER_EXECUTOR_B.submit(new VoucherOrderHandler2());
    }
    private class VoucherOrderHandler2 implements Runnable {

        @Override
        public void run() {
            while (true) {
                // 1.获取消息队列中的订单信息 XREADGROUP GROUP g1 c1 COUNT 1 BLOCK 2000 STREAMS s1 >
                try {
                    List<MapRecord<String, Object, Object>> list = stringRedisTemplate.opsForStream().read(Consumer.from("g1", "c1"),
                            StreamReadOptions.empty().count(1).block(Duration.ofSeconds(2)),
                            StreamOffset.create("stream.orders", ReadOffset.lastConsumed()));
                    // 2.判断订单信息是否为空
                    if (list == null || list.isEmpty()) {
                        // 如果为null，说明没有消息，继续下一次循环
                        continue;
                    }
                    // 解析数据
                    MapRecord<String, Object, Object> record = list.get(0);
                    Map<Object, Object> value = record.getValue();
                    VoucherOrder voucherOrder = BeanUtil.fillBeanWithMap(value, new VoucherOrder(), true);
                    // 3.创建订单
                    proxy2.createVoucherOrder1(voucherOrder);
                    // 4.确认消息 XACK
                    stringRedisTemplate.opsForStream().acknowledge("stream.orders", "g1", record.getId());
                    //处理异常消息
                } catch (Exception e) {
                    log.error("处理异常信息");
                    handlePendingList();
                }
            }
        }
    }
    private  IVoucherOrderService proxy2;
    @Override
    public Result seckillVoucher2(Long voucherId) {
        Long userId = UserHolder.getUser().getId();
        Long orderId = redisIdWorker.nextId("order");
        Long result = stringRedisTemplate.execute(SECKILL_SCRIPT,
                Collections.emptyList(),
                voucherId.toString(), userId.toString(), String.valueOf(orderId));
        int r = result.intValue();
        if (r != 0){
            return Result.fail(r == 1 ? "库存不足" : "重复下单");
        }
        proxy2 = (IVoucherOrderService) AopContext.currentProxy();
        return Result.ok(orderId);
    }

    private void handlePendingList(){
        while (true) {
            try {
                // 1.获取pending-list中的订单信息 XREADGROUP GROUP g1 c1 COUNT 1 BLOCK 2000 STREAMS s1 0
                List<MapRecord<String, Object, Object>> list = stringRedisTemplate.opsForStream().
                        read(Consumer.from("g1", "c1"),
                                StreamReadOptions.empty().count(1),
                                StreamOffset.create("stream.orders", ReadOffset.from("0")));
//                判断是否为空
                if (list == null || list.isEmpty()) {
                    break;
                }
                MapRecord<String, Object, Object> record = list.get(0);
                Map<Object, Object> value = record.getValue();
                VoucherOrder voucherOrder = BeanUtil.fillBeanWithMap(value, new VoucherOrder(), true);
//                创建订单
               handleVoucherOrder(voucherOrder);
//                确认消息
                stringRedisTemplate.opsForStream().acknowledge("stream.orders", "g1", record.getId());
            } catch (Exception e) {
                log.error("处理异常订单",e);
                try{
                    Thread.sleep(20);
                }catch(InterruptedException interruptedException){
                    interruptedException.printStackTrace();
                }
            }
        }
    }
}
